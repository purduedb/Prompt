
package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.prompt.dstream.ReceiverInputDStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.prompt.{Seconds, StreamingContext}

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 *
 * Usage: PromptWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.PromptWordCount localhost 9999`
 */
object PromptWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: PromptWordCount <hostname> <port>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("PromptWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    // Set the partitioner type
    sparkConf.setExecutorEnv("Partitioner","PromptPartitoner" )
    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
