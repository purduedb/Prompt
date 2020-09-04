/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.util.{Clock, UninterruptibleThread, Utils}

/**
 * Manages the execution of a streaming Spark SQL query that is occurring in a separate thread.
 * Unlike a standard query, a streaming query executes repeatedly each time new data arrives at any
 * [[Source]] present in the query plan. Whenever new data arrives, a [[QueryExecution]] is created
 * and the results are committed transactionally to the given [[Sink]].
 */
class StreamExecution(
    override val sparkSession: SparkSession,
    override val id: Long,
    override val name: String,
    checkpointRoot: String,
    val logicalPlan: LogicalPlan,
    val sink: Sink,
    val trigger: Trigger,
    val triggerClock: Clock,
    val outputMode: OutputMode)
  extends StreamingQuery with Logging {

  import org.apache.spark.sql.streaming.StreamingQueryListener._
  import StreamMetrics._

  private val pollingDelayMs = sparkSession.sessionState.conf.streamingPollingDelay

  /**
   * A lock used to wait/notify when batches complete. Use a fair lock to avoid thread starvation.
   */
  private val awaitBatchLock = new ReentrantLock(true)
  private val awaitBatchLockCondition = awaitBatchLock.newCondition()

  private val startLatch = new CountDownLatch(1)
  private val terminationLatch = new CountDownLatch(1)

  /**
   * Tracks how much data we have processed and committed to the sink or state store from each
   * input source.
   * Only the scheduler thread should modify this field, and only in atomic steps.
   * Other threads should make a shallow copy if they are going to access this field more than
   * once, since the field's value may change at any time.
   */
  @volatile
  var committedOffsets = new StreamProgress

  /**
   * Tracks the offsets that are available to be processed, but have not yet be committed to the
   * sink.
   * Only the scheduler thread should modify this field, and only in atomic steps.
   * Other threads should make a shallow copy if they are going to access this field more than
   * once, since the field's value may change at any time.
   */
  @volatile
  private var availableOffsets = new StreamProgress

  /** The current batchId or -1 if execution has not yet been initialized. */
  private var currentBatchId: Long = -1

  /** All stream sources present in the query plan. */
  private val sources =
    logicalPlan.collect { case s: StreamingExecutionRelation => s.source }

  /** A list of unique sources in the query plan. */
  private val uniqueSources = sources.distinct

  private val triggerExecutor = trigger match {
    case t: ProcessingTime => ProcessingTimeExecutor(t, triggerClock)
  }

  /** Defines the internal state of execution */
  @volatile
  private var state: State = INITIALIZED

  @volatile
  var lastExecution: QueryExecution = null

  @volatile
  private var streamDeathCause: StreamingQueryException = null

  /* Get the call site in the caller thread; will pass this into the micro batch thread */
  private val callSite = Utils.getCallSite()

  /** Metrics for this query */
  private val streamMetrics =
    new StreamMetrics(uniqueSources.toSet, triggerClock, s"StructuredStreaming.$name")

  @volatile
  private var currentStatus: StreamingQueryStatus = null

  /** Flag that signals whether any error with input metrics have already been logged */
  @volatile
  private var metricWarningLogged: Boolean = false

  /**
   * The thread that runs the micro-batches of this stream. Note that this thread must be
   * [[org.apache.spark.util.UninterruptibleThread]] to avoid potential deadlocks in using
   * [[HDFSMetadataLog]]. See SPARK-14131 for more details.
   */
  val microBatchThread =
    new StreamExecutionThread(s"stream execution thread for $name") {
      override def run(): Unit = {
        // To fix call site like "run at <unknown>:0", we bridge the call site from the caller
        // thread to this micro batch thread
        sparkSession.sparkContext.setCallSite(callSite)
        runBatches()
      }
    }

  /**
   * A write-ahead-log that records the offsets that are present in each batch. In order to ensure
   * that a given batch will always consist of the same data, we write to this log *before* any
   * processing is done.  Thus, the Nth record in this log indicated data that is currently being
   * processed and the N-1th entry indicates which offsets have been durably committed to the sink.
   */
  val offsetLog = new HDFSMetadataLog[CompositeOffset](sparkSession, checkpointFile("offsets"))

  /** Whether the query is currently active or not */
  override def isActive: Boolean = state == ACTIVE

  /** Returns the current status of the query. */
  override def status: StreamingQueryStatus = currentStatus

  /** Returns current status of all the sources. */
  override def sourceStatuses: Array[SourceStatus] = currentStatus.sourceStatuses.toArray

  /** Returns current status of the sink. */
  override def sinkStatus: SinkStatus = currentStatus.sinkStatus

  /** Returns the [[StreamingQueryException]] if the query was terminated by an exception. */
  override def exception: Option[StreamingQueryException] = Option(streamDeathCause)

  /** Returns the path of a file with `name` in the checkpoint directory. */
  private def checkpointFile(name: String): String =
    new Path(new Path(checkpointRoot), name).toUri.toString

  /**
   * Starts the execution. This returns only after the thread has started and [[QueryStartedEvent]]
   * has been posted to all the listeners.
   */
  def start(): Unit = {
    microBatchThread.setDaemon(true)
    microBatchThread.start()
    startLatch.await()  // Wait until thread started and QueryStart event has been posted
  }

  /**
   * Repeatedly attempts to run batches as data arrives.
   *
   * Note that this method ensures that [[QueryStartedEvent]] and [[QueryTerminatedEvent]] are
   * posted such that listeners are guaranteed to get a start event before a termination.
   * Furthermore, this method also ensures that [[QueryStartedEvent]] event is posted before the
   * `start()` method returns.
   */
  private def runBatches(): Unit = {
    try {
      // Mark ACTIVE and then post the event. QueryStarted event is synchronously sent to listeners,
      // so must mark this as ACTIVE first.
      state = ACTIVE
      if (sparkSession.sessionState.conf.streamingMetricsEnabled) {
        sparkSession.sparkContext.env.metricsSystem.registerSource(streamMetrics)
      }
      updateStatus()
      postEvent(new QueryStartedEvent(currentStatus)) // Assumption: Does not throw exception.

      // Unblock starting thread
      startLatch.countDown()

      // While active, repeatedly attempt to run batches.
      SparkSession.setActiveSession(sparkSession)

      triggerExecutor.execute(() => {
        streamMetrics.reportTriggerStarted(currentBatchId)
        streamMetrics.reportTriggerDetail(STATUS_MESSAGE, "Finding new data from sources")
        updateStatus()
        val isTerminated = reportTimeTaken(TRIGGER_LATENCY) {
          if (isActive) {
            if (currentBatchId < 0) {
              // We'll do this initialization only once
              populateStartOffsets()
              logDebug(s"Stream running from $committedOffsets to $availableOffsets")
            } else {
              constructNextBatch()
            }
            if (dataAvailable) {
              streamMetrics.reportTriggerDetail(IS_DATA_PRESENT_IN_TRIGGER, true)
              streamMetrics.reportTriggerDetail(STATUS_MESSAGE, "Processing new data")
              updateStatus()
              runBatch()
              // We'll increase currentBatchId after we complete processing current batch's data
              currentBatchId += 1
            } else {
              streamMetrics.reportTriggerDetail(IS_DATA_PRESENT_IN_TRIGGER, false)
              streamMetrics.reportTriggerDetail(STATUS_MESSAGE, "No new data")
              updateStatus()
              Thread.sleep(pollingDelayMs)
            }
            true
          } else {
            false
          }
        }
        // Update metrics and notify others
        streamMetrics.reportTriggerFinished()
        updateStatus()
        postEvent(new QueryProgressEvent(currentStatus))
        isTerminated
      })
    } catch {
      case _: InterruptedException if state == TERMINATED => // interrupted by stop()
      case e: Throwable =>
        streamDeathCause = new StreamingQueryException(
          this,
          s"Query $name terminated with exception: ${e.getMessage}",
          e,
          Some(committedOffsets.toCompositeOffset(sources)))
        logError(s"Query $name terminated with error", e)
        // Rethrow the fatal errors to allow the user using `Thread.UncaughtExceptionHandler` to
        // handle them
        if (!NonFatal(e)) {
          throw e
        }
    } finally {
      state = TERMINATED

      // Update metrics and status
      streamMetrics.stop()
      sparkSession.sparkContext.env.metricsSystem.removeSource(streamMetrics)
      updateStatus()

      // Notify others
      sparkSession.streams.notifyQueryTermination(StreamExecution.this)
      postEvent(
        new QueryTerminatedEvent(currentStatus, exception.map(_.cause).map(Utils.exceptionString)))
      terminationLatch.countDown()
    }
  }

  /**
   * Populate the start offsets to start the execution at the current offsets stored in the sink
   * (i.e. avoid reprocessing data that we have already processed). This function must be called
   * before any processing occurs and will populate the following fields:
   *  - currentBatchId
   *  - committedOffsets
   *  - availableOffsets
   */
  private def populateStartOffsets(): Unit = {
    offsetLog.getLatest() match {
      case Some((batchId, nextOffsets)) =>
        logInfo(s"Resuming streaming query, starting with batch $batchId")
        currentBatchId = batchId
        availableOffsets = nextOffsets.toStreamProgress(sources)
        logDebug(s"Found possibly uncommitted offsets $availableOffsets")

        offsetLog.get(batchId - 1).foreach {
          case lastOffsets =>
            committedOffsets = lastOffsets.toStreamProgress(sources)
            logDebug(s"Resuming with committed offsets: $committedOffsets")
        }
      case None => // We are starting this stream for the first time.
        logInfo(s"Starting new streaming query.")
        currentBatchId = 0
        constructNextBatch()
    }
  }

  /**
   * Returns true if there is any new data available to be processed.
   */
  private def dataAvailable: Boolean = {
    availableOffsets.exists {
      case (source, available) =>
        committedOffsets
            .get(source)
            .map(committed => committed != available)
            .getOrElse(true)
    }
  }

  /**
   * Queries all of the sources to see if any new data is available. When there is new data the
   * batchId counter is incremented and a new log entry is written with the newest offsets.
   */
  private def constructNextBatch(): Unit = {
    // Check to see what new data is available.
    val hasNewData = {
      awaitBatchLock.lock()
      try {
        reportTimeTaken(GET_OFFSET_LATENCY) {
          val latestOffsets: Map[Source, Option[Offset]] = uniqueSources.map { s =>
            reportTimeTaken(s, SOURCE_GET_OFFSET_LATENCY) {
              (s, s.getOffset)
            }
          }.toMap
          availableOffsets ++= latestOffsets.filter { case (s, o) => o.nonEmpty }.mapValues(_.get)
        }

        if (dataAvailable) {
          true
        } else {
          noNewData = true
          false
        }
      } finally {
        awaitBatchLock.unlock()
      }
    }
    if (hasNewData) {
      reportTimeTaken(OFFSET_WAL_WRITE_LATENCY) {
        assert(offsetLog.add(currentBatchId, availableOffsets.toCompositeOffset(sources)),
          s"Concurrent update to the log. Multiple streaming jobs detected for $currentBatchId")
        logInfo(s"Committed offsets for batch $currentBatchId.")

        // NOTE: The following code is correct because runBatches() processes exactly one
        // batch at a time. If we add pipeline parallelism (multiple batches in flight at
        // the same time), this cleanup logic will need to change.

        // Now that we've updated the scheduler's persistent checkpoint, it is safe for the
        // sources to discard data from the previous batch.
        val prevBatchOff = offsetLog.get(currentBatchId - 1)
        if (prevBatchOff.isDefined) {
          prevBatchOff.get.toStreamProgress(sources).foreach {
            case (src, off) => src.commit(off)
          }
        }

        // Now that we have logged the new batch, no further processing will happen for
        // the batch before the previous batch, and it is safe to discard the old metadata.
        // Note that purge is exclusive, i.e. it purges everything before the target ID.
        offsetLog.purge(currentBatchId - 1)
      }
    } else {
      awaitBatchLock.lock()
      try {
        // Wake up any threads that are waiting for the stream to progress.
        awaitBatchLockCondition.signalAll()
      } finally {
        awaitBatchLock.unlock()
      }
    }
    reportTimestamp(GET_OFFSET_TIMESTAMP)
  }

  /**
   * Processes any data available between `availableOffsets` and `committedOffsets`.
   */
  private def runBatch(): Unit = {
    // TODO: Move this to IncrementalExecution.

    // Request unprocessed data from all sources.
    val newData = reportTimeTaken(GET_BATCH_LATENCY) {
      availableOffsets.flatMap {
        case (source, available)
          if committedOffsets.get(source).map(_ != available).getOrElse(true) =>
          val current = committedOffsets.get(source)
          val batch = reportTimeTaken(source, SOURCE_GET_BATCH_LATENCY) {
            source.getBatch(current, available)
          }
          logDebug(s"Retrieving data from $source: $current -> $available")
          Some(source -> batch)
        case _ => None
      }
    }
    reportTimestamp(GET_BATCH_TIMESTAMP)

    // A list of attributes that will need to be updated.
    var replacements = new ArrayBuffer[(Attribute, Attribute)]
    // Replace sources in the logical plan with data that has arrived since the last batch.
    val withNewSources = logicalPlan transform {
      case StreamingExecutionRelation(source, output) =>
        newData.get(source).map { data =>
          val newPlan = data.logicalPlan
          assert(output.size == newPlan.output.size,
            s"Invalid batch: ${Utils.truncatedString(output, ",")} != " +
            s"${Utils.truncatedString(newPlan.output, ",")}")
          replacements ++= output.zip(newPlan.output)
          newPlan
        }.getOrElse {
          LocalRelation(output)
        }
    }

    // Rewire the plan to use the new attributes that were returned by the source.
    val replacementMap = AttributeMap(replacements)
    val triggerLogicalPlan = withNewSources transformAllExpressions {
      case a: Attribute if replacementMap.contains(a) => replacementMap(a)
    }

    val executedPlan = reportTimeTaken(OPTIMIZER_LATENCY) {
      lastExecution = new IncrementalExecution(
        sparkSession,
        triggerLogicalPlan,
        outputMode,
        checkpointFile("state"),
        currentBatchId)
      lastExecution.executedPlan // Force the lazy generation of execution plan
    }

    val nextBatch =
      new Dataset(sparkSession, lastExecution, RowEncoder(lastExecution.analyzed.schema))
    sink.addBatch(currentBatchId, nextBatch)
    reportNumRows(executedPlan, triggerLogicalPlan, newData)

    awaitBatchLock.lock()
    try {
      // Wake up any threads that are waiting for the stream to progress.
      awaitBatchLockCondition.signalAll()
    } finally {
      awaitBatchLock.unlock()
    }

    // Update committed offsets.
    committedOffsets ++= availableOffsets
  }

  private def postEvent(event: StreamingQueryListener.Event) {
    sparkSession.streams.postListenerEvent(event)
  }

  /**
   * Signals to the thread executing micro-batches that it should stop running after the next
   * batch. This method blocks until the thread stops running.
   */
  override def stop(): Unit = {
    // Set the state to TERMINATED so that the batching thread knows that it was interrupted
    // intentionally
    state = TERMINATED
    if (microBatchThread.isAlive) {
      microBatchThread.interrupt()
      microBatchThread.join()
    }
    uniqueSources.foreach(_.stop())
    logInfo(s"Query $name was stopped")
  }

  /**
   * Blocks the current thread until processing for data from the given `source` has reached at
   * least the given `Offset`. This method is intended for use primarily when writing tests.
   */
  private[sql] def awaitOffset(source: Source, newOffset: Offset): Unit = {
    def notDone = {
      val localCommittedOffsets = committedOffsets
      !localCommittedOffsets.contains(source) || localCommittedOffsets(source) != newOffset
    }

    while (notDone) {
      awaitBatchLock.lock()
      try {
        awaitBatchLockCondition.await(100, TimeUnit.MILLISECONDS)
        if (streamDeathCause != null) {
          throw streamDeathCause
        }
      } finally {
        awaitBatchLock.unlock()
      }
    }
    logDebug(s"Unblocked at $newOffset for $source")
  }

  /** A flag to indicate that a batch has completed with no new data available. */
  @volatile private var noNewData = false

  override def processAllAvailable(): Unit = {
    awaitBatchLock.lock()
    try {
      noNewData = false
      while (true) {
        awaitBatchLockCondition.await(10000, TimeUnit.MILLISECONDS)
        if (streamDeathCause != null) {
          throw streamDeathCause
        }
        if (noNewData) {
          return
        }
      }
    } finally {
      awaitBatchLock.unlock()
    }
  }

  override def awaitTermination(): Unit = {
    if (state == INITIALIZED) {
      throw new IllegalStateException("Cannot wait for termination on a query that has not started")
    }
    terminationLatch.await()
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
  }

  override def awaitTermination(timeoutMs: Long): Boolean = {
    if (state == INITIALIZED) {
      throw new IllegalStateException("Cannot wait for termination on a query that has not started")
    }
    require(timeoutMs > 0, "Timeout has to be positive")
    terminationLatch.await(timeoutMs, TimeUnit.MILLISECONDS)
    if (streamDeathCause != null) {
      throw streamDeathCause
    } else {
      !isActive
    }
  }

  /** Expose for tests */
  def explainInternal(extended: Boolean): String = {
    if (lastExecution == null) {
      "No physical plan. Waiting for data."
    } else {
      val explain = ExplainCommand(lastExecution.logical, extended = extended)
      sparkSession.sessionState.executePlan(explain).executedPlan.executeCollect()
        .map(_.getString(0)).mkString("\n")
    }
  }

  override def explain(extended: Boolean): Unit = {
    // scalastyle:off println
    println(explainInternal(extended))
    // scalastyle:on println
  }

  override def explain(): Unit = explain(extended = false)

  override def toString: String = {
    s"Streaming Query - $name [state = $state]"
  }

  def toDebugString: String = {
    val deathCauseStr = if (streamDeathCause != null) {
      "Error:\n" + stackTraceToString(streamDeathCause.cause)
    } else ""
    s"""
       |=== Streaming Query ===
       |Name: $name
       |Current Offsets: $committedOffsets
       |
       |Current State: $state
       |Thread State: ${microBatchThread.getState}
       |
       |Logical Plan:
       |$logicalPlan
       |
       |$deathCauseStr
     """.stripMargin
  }

  /**
   * Report row metrics of the executed trigger
   * @param triggerExecutionPlan Execution plan of the trigger
   * @param triggerLogicalPlan Logical plan of the trigger, generated from the query logical plan
   * @param sourceToDF Source to DataFrame returned by the source.getBatch in this trigger
   */
  private def reportNumRows(
      triggerExecutionPlan: SparkPlan,
      triggerLogicalPlan: LogicalPlan,
      sourceToDF: Map[Source, DataFrame]): Unit = {
    // We want to associate execution plan leaves to sources that generate them, so that we match
    // the their metrics (e.g. numOutputRows) to the sources. To do this we do the following.
    // Consider the translation from the streaming logical plan to the final executed plan.
    //
    //  streaming logical plan (with sources) <==> trigger's logical plan <==> executed plan
    //
    // 1. We keep track of streaming sources associated with each leaf in the trigger's logical plan
    //    - Each logical plan leaf will be associated with a single streaming source.
    //    - There can be multiple logical plan leaves associated with a streaming source.
    //    - There can be leaves not associated with any streaming source, because they were
    //      generated from a batch source (e.g. stream-batch joins)
    //
    // 2. Assuming that the executed plan has same number of leaves in the same order as that of
    //    the trigger logical plan, we associate executed plan leaves with corresponding
    //    streaming sources.
    //
    // 3. For each source, we sum the metrics of the associated execution plan leaves.
    //
    val logicalPlanLeafToSource = sourceToDF.flatMap { case (source, df) =>
      df.logicalPlan.collectLeaves().map { leaf => leaf -> source }
    }
    val allLogicalPlanLeaves = triggerLogicalPlan.collectLeaves() // includes non-streaming sources
    val allExecPlanLeaves = triggerExecutionPlan.collectLeaves()
    val sourceToNumInputRows: Map[Source, Long] =
      if (allLogicalPlanLeaves.size == allExecPlanLeaves.size) {
        val execLeafToSource = allLogicalPlanLeaves.zip(allExecPlanLeaves).flatMap {
          case (lp, ep) => logicalPlanLeafToSource.get(lp).map { source => ep -> source }
        }
        val sourceToNumInputRows = execLeafToSource.map { case (execLeaf, source) =>
          val numRows = execLeaf.metrics.get("numOutputRows").map(_.value).getOrElse(0L)
          source -> numRows
        }
        sourceToNumInputRows.groupBy(_._1).mapValues(_.map(_._2).sum) // sum up rows for each source
      } else {
        if (!metricWarningLogged) {
          def toString[T](seq: Seq[T]): String = s"(size = ${seq.size}), ${seq.mkString(", ")}"
          logWarning(
            "Could not report metrics as number leaves in trigger logical plan did not match that" +
              s" of the execution plan:\n" +
              s"logical plan leaves: ${toString(allLogicalPlanLeaves)}\n" +
              s"execution plan leaves: ${toString(allExecPlanLeaves)}\n")
          metricWarningLogged = true
        }
        Map.empty
      }
    val numOutputRows = triggerExecutionPlan.metrics.get("numOutputRows").map(_.value)
    val stateNodes = triggerExecutionPlan.collect {
      case p if p.isInstanceOf[StateStoreSaveExec] => p
    }

    streamMetrics.reportNumInputRows(sourceToNumInputRows)
    stateNodes.zipWithIndex.foreach { case (s, i) =>
      streamMetrics.reportTriggerDetail(
        NUM_TOTAL_STATE_ROWS(i + 1),
        s.metrics.get("numTotalStateRows").map(_.value).getOrElse(0L))
      streamMetrics.reportTriggerDetail(
        NUM_UPDATED_STATE_ROWS(i + 1),
        s.metrics.get("numUpdatedStateRows").map(_.value).getOrElse(0L))
    }
    updateStatus()
  }

  private def reportTimeTaken[T](triggerDetailKey: String)(body: => T): T = {
    val startTime = triggerClock.getTimeMillis()
    val result = body
    val endTime = triggerClock.getTimeMillis()
    val timeTaken = math.max(endTime - startTime, 0)
    streamMetrics.reportTriggerDetail(triggerDetailKey, timeTaken)
    updateStatus()
    if (triggerDetailKey == TRIGGER_LATENCY) {
      logInfo(s"Completed up to $availableOffsets in $timeTaken ms")
    }
    result
  }

  private def reportTimeTaken[T](source: Source, triggerDetailKey: String)(body: => T): T = {
    val startTime = triggerClock.getTimeMillis()
    val result = body
    val endTime = triggerClock.getTimeMillis()
    streamMetrics.reportSourceTriggerDetail(
      source, triggerDetailKey, math.max(endTime - startTime, 0))
    updateStatus()
    result
  }

  private def reportTimestamp(triggerDetailKey: String): Unit = {
    streamMetrics.reportTriggerDetail(triggerDetailKey, triggerClock.getTimeMillis)
    updateStatus()
  }

  private def updateStatus(): Unit = {
    val localAvailableOffsets = availableOffsets
    val sourceStatuses = sources.map { s =>
      SourceStatus(
        s.toString,
        localAvailableOffsets.get(s).map(_.toString).getOrElse("-"), // TODO: use json if available
        streamMetrics.currentSourceInputRate(s),
        streamMetrics.currentSourceProcessingRate(s),
        streamMetrics.currentSourceTriggerDetails(s))
    }.toArray
    val sinkStatus = SinkStatus(
      sink.toString,
      committedOffsets.toCompositeOffset(sources).toString)

    currentStatus =
      StreamingQueryStatus(
        name = name,
        id = id,
        timestamp = triggerClock.getTimeMillis(),
        inputRate = streamMetrics.currentInputRate(),
        processingRate = streamMetrics.currentProcessingRate(),
        latency = streamMetrics.currentLatency(),
        sourceStatuses = sourceStatuses,
        sinkStatus = sinkStatus,
        triggerDetails = streamMetrics.currentTriggerDetails())
  }

  trait State
  case object INITIALIZED extends State
  case object ACTIVE extends State
  case object TERMINATED extends State
}

object StreamExecution {
  private val _nextId = new AtomicLong(0)

  def nextId: Long = _nextId.getAndIncrement()
}

/**
 * A special thread to run the stream query. Some codes require to run in the StreamExecutionThread
 * and will use `classOf[StreamExecutionThread]` to check.
 */
abstract class StreamExecutionThread(name: String) extends UninterruptibleThread(name)
