# Prompt

In this project, we introduce efficient data partitioning technique, where incoming batches are partitioned evenly for the map and reduce tasks. 

The repository is built over Apache Spark v2.0.0. The prototype exposes the low level API of Spark and uses the `runJobs` method of [SparkContext](core/src/main/scala/org/apache/spark/SparkContext.scala). To try , first build Spark based on existing [instructions](http://spark.apache.org/docs/latest/building-spark.html). For example, using SBT we can run
```
  ./build/sbt package
```

## Usage

You need to specify "PromptPartitioner" for the execution environment as follows:

    sparkConf.setExecutorEnv("Partitioner","PromptPartitoner" )

When running multiple computations as part of one app then the number of mappers is automatically detected using the number of data blocks (i.e., partitions). However, you need to specify the number of reducers in your computation when initiating the PromptPartitioner object as follows: 

      val partitioner = new PromptPartitioner(numReducers)

Please check  ```org.apache.spark.examples.PromptExample``` for more details.

## Example
You can run the `PromptWordCount` example with 4 cores for 10 batches with our proposed technique. Note that this example requires at least 4GB of memory on your machine.
```
  ./bin/run-example --master "local-cluster[4,1,1024]" org.apache.spark.examples.PromptWordCount
```
To compare this with existing Spark, we can run the same computation but with default Spark partitioner (time-based)
```
  ./bin/run-example --master "local-cluster[4,1,1024]" org.apache.spark.examples.StreamWordCount
```

The benefit from using our data partitioning technique is clear on larger clusters. Results from running the two stage query for different workloads and different batch interval sizes on
[Amazon EC2 cluster](spark.apache.org/docs/latest/ec2-scripts.html) is presented in our paper.


## Status
The source code in this repository is a research prototype and only implements the data partitioning described in our paper. We are working on adding more features to our work.


## Publication

* Ahmed S. Abdelhamid, Ahmed R. Mahmood, Anas Daghistani, Walid G. Aref, “Prompt: Dynamic Data Partitioning for Distributed Micro-batch Stream Processing Systems”, In proceedings of International Conference on Management of Data, June 14-19, 2020


## Contact

If you have any question please feel free to send an email. 

* Ahmed S. Abdelhamid <samy@purdue.edu>