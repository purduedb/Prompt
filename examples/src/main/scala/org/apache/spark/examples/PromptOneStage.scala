package org.apache.spark.examples

import scala.math.random

import org.apache.spark._

object PromptOneStage {

  def main(args: Array[String]) {

    val numElemsPerPart = 1000L

    val numIters = if (args.length > 0) args(0).toInt else 10
    val batchSize = if (args.length > 1) args(1).toInt else 5 // if batchSize is 1 it runs baseline
    val partitions = if (args.length > 2) args(2).toInt else 4 // number of partitions of the job
    val numTrials = if (args.length > 3) args(3).toInt else 5
    val numElems = if (args.length > 4) args(4).toLong else (numElemsPerPart * partitions)

    println(s"Running PromptSingleStage for $numIters iterations " +
      s"with $batchSize batchSize, $partitions partitions " +
      s"for $numTrials trials, $numElems total number of elements")

    val conf = new SparkConf().setAppName("PromptOneStage")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // Let all the executors join
    Thread.sleep(10000)
    // Warm up the JVM and copy the JAR out to all the machines etc.
    sc.parallelize(0 until sc.getExecutorMemoryStatus.size,
      sc.getExecutorMemoryStatus.size).foreach { x =>
      Thread.sleep(1)
    }

    val rdd = sc.parallelize(1L to numElems, partitions).map(i => 2L * i).cache()
    rdd.count()

    val sumFunc = (iter: Iterator[Long]) => iter.reduceLeft(_ + _)

    for (j <- 0 until numTrials) {
      val begin = System.nanoTime
      val sums = if (batchSize == 1) {
        (0 until numIters).map { i =>
          sc.runJob(rdd.map(x => x + i), sumFunc)
        }
      } else {
        val numBatches = math.ceil(numIters.toDouble / batchSize).toInt
        val funcs = Seq.fill(batchSize)(sumFunc)
        // Use a different RDD for each trial
        val rdds = (0 until batchSize).map { i => rdd.map(x => x + i) }
        (0 until numBatches).flatMap { b =>
          val outs = sc.runJobs(rdds, funcs)
          outs
        }
      }
      val end = System.nanoTime
      println("Got sum " + sums.map(x => x.sum).sum)
      println("Drizzle: Running " + numIters + " iters " + batchSize + " batchSize took " +
        (end-begin)/1e6 + " ms")
    }

    sc.stop()
  }
}
