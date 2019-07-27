package com.waitingforcode.streaming

import java.util.concurrent.TimeUnit

import com.waitingforcode.core.SessionOutput
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.{Dataset, SparkSession, functions}

object Application {

  def main(args: Array[String]): Unit = {
    val outputDir = args(0)
    val kafkaConfiguration = KafkaConfiguration.createFromConfigurationFile(args(1))

    val sparkSession = SparkSession.builder()
      .appName("Sessionization-demo: streaming approach").master("local[*]").getOrCreate()
    import sparkSession.implicits._

    val dataFrame = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfiguration.broker)
      // TODO: during the talk show some myths about checkpointing (only growing place? - no because of the purge)
      // (constraints - cannot change the data source & so forth)
      .option("checkpointLocation", s"/tmp/sessionization-demo-streaming/checkpoint")
      .option("client.id", s"sessionization-demo-streaming")
      .option("subscribe", kafkaConfiguration.inputTopic)
      .load()

    val sessionTimeout = TimeUnit.MINUTES.toMillis(5)
    val query = dataFrame.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING)")
      .groupByKey(row => row.getAs[String]("key"))
      .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(Mapping.mapStreamingLogsToSessions(sessionTimeout))

    val writeQuery = query.writeStream.foreachBatch(BatchWriter.writeBatch(outputDir) _).start()

    writeQuery.awaitTermination()
  }

}

object BatchWriter {

  def writeBatch(outputDir: String)(batchDataset: Dataset[Option[Iterator[SessionOutput]]], batchId: Long) = {
    batchDataset.filter(state => state.isDefined)
      .withColumn("year", functions.year(functions.col("eventTime")))
      .withColumn("month", functions.month(functions.col("eventTime")))
      .withColumn("day", functions.dayofmonth(functions.col("eventTime")))
      .withColumn("hour", functions.hour(functions.col("eventTime")))
      .write
      // TODO: add a suffixed partition column to avoid uneven distribution?
      .partitionBy("year, month, day, hour")
      .json(outputDir)
  }

}