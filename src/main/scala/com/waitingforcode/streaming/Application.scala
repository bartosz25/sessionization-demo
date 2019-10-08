package com.waitingforcode.streaming

import java.util.concurrent.TimeUnit

import com.waitingforcode.core.{BatchWriter, SessionIntermediaryState, Visit}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession, functions}

/**
  * Streaming implementation for the sessionization problem.
  * Tips & tricks:
  * - set `spark.sql.shuffle.partitions` to 1 if you want to explore checkpointed files easier
  * - set `maxOffsetsPerTrigger` on the data source to something low like 5 if you want to
  *   observe how sessions and states evolve
  * - use `.format("console").option("truncate", "false")` in the sink instead of `foreachBatch`
  *   if you want to observe how your data change in real time
  */
object Application {

  def main(args: Array[String]): Unit = {
    val outputDir = args(0)
    val kafkaConfiguration = KafkaConfiguration.createFromConfigurationFile(args(1))

    val sparkSession = SparkSession.builder()
      .appName("Sessionization-demo: streaming approach").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val dataFrame = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfiguration.broker)
      .option("client.id", s"sessionization-demo-streaming")
      .option("subscribe", kafkaConfiguration.inputTopic)
      .option("startingOffsets", kafkaConfiguration.startingOffset)
      .load()

    val sessionTimeout = TimeUnit.MINUTES.toMillis(5)
    val query = dataFrame.selectExpr("CAST(value AS STRING)")
      .select(functions.from_json($"value", Visit.Schema).as("data"))
      .select($"data.*")
      .withWatermark("event_time", "10 minutes")
      .groupByKey(row => row.getAs[Long]("user_id"))
      .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(Mapping.mapStreamingLogsToSessions(sessionTimeout))

    val writeQuery = query.writeStream.outputMode(OutputMode.Update())
      .option("checkpointLocation", s"/tmp/sessionization-demo-streaming/checkpoint")
      .foreachBatch((dataset: Dataset[SessionIntermediaryState], batchId: Long) => {
        BatchWriter.writeDataset(dataset, s"${outputDir}/${batchId}")
      }).start()

    writeQuery.awaitTermination()
  }

}