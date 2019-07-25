package com.waitingforcode.streaming

import com.waitingforcode.core.SessionOutput
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.{Dataset, SparkSession}

object Application {

  def main(args: Array[String]): Unit = {
    val outputDir = args(0)

    val sparkSession = SparkSession.builder()
      .appName("Sessionization-demo: streaming approach").master("local[*]").getOrCreate()
    import sparkSession.implicits._

    val dataFrame = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaConfiguration.Broker)
      // TODO: during the talk show some myths about checkpointing (only growing place? - no because of the purge)
      // (constraints - cannot change the data source & so forth)
      .option("checkpointLocation", s"/tmp/sessionization-demo-streaming/checkpoint")
      .option("client.id", s"sessionization-demo-streaming")
      .option("subscribe", KafkaConfiguration.InputTopic)
      .load()

    val query = dataFrame.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING)")
      .groupByKey(row => row.getAs[String]("key"))
      .mapGroupsWithState(timeoutConf = GroupStateTimeout.EventTimeTimeout())(Mapping.mapStreamingLogsToSessions)

    val writeQuery = query.writeStream.foreachBatch(BatchWriter.writeBatch(outputDir) _).start()

    writeQuery.awaitTermination()
  }

}

object BatchWriter {

  def writeBatch(outputDir: String)(batchDataset: Dataset[Option[Iterator[SessionOutput]]], batchId: Long) = {
    batchDataset.filter(state => state.isDefined)
      .write
      // TODO: add a suffixed partition column to avoid uneven distribution?
      .partitionBy("year, month, day, hour")
      .json(outputDir)
  }

}