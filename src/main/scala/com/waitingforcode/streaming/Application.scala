package com.waitingforcode.streaming

import java.util.concurrent.TimeUnit

import com.waitingforcode.core.{SessionIntermediaryState, Visit}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession, functions}

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
      // TODO: ensure that Spark doesn't create the topic if it doesn't exist !
      .option("subscribe", kafkaConfiguration.inputTopic)
      // TODO: schema problem. We can't set a custom schema for Kafka source simply because of this error:
      // Exception in thread "main" java.lang.IllegalArgumentException: requirement failed: Kafka source has a fixed schema and cannot be set with a custom one
      //	at scala.Predef$.require(Predef.scala:277)
      //	at org.apache.spark.sql.kafka010.KafkaSourceProvider.sourceSchema(KafkaSourceProvider.scala:67)
      // .schema(Visit.Schema)
      .load()

    val sessionTimeout =  TimeUnit.SECONDS.toMillis(40) // TODO: put back this property  TimeUnit.MINUTES.toMillis(5)
    val query = dataFrame.selectExpr("CAST(value AS STRING)", "timestamp")
      // TODO: the conversion idea came from https://dzone.com/articles/basic-example-for-spark-structured-streaming-amp-k
      .select(functions.from_json($"value", Visit.Schema).as("data"), $"timestamp")
      .select($"data.*", $"timestamp")
      // TODO: I'm using here the watermark of the input. I'm not sure how it will behave with the state expiration?
      //       ^----- to solve that issue, add a blog post and show it as an image
      //              source for timestamp column: https://stackoverflow.com/questions/54298251/how-to-include-kafka-timestamp-value-as-columns-in-spark-structured-streaming
      .withWatermark("timestamp", "3 minutes")
      .groupByKey(row => row.getAs[Long]("user_id"))
      // FlatMapGroupsWithState org.apache.spark.sql.KeyValueGroupedDataset$$Lambda$1050/441021062@384472bf, cast(value#26 as string).toString, createexternalrow(key#21.toString, value#22.toString, StructField(key,StringType,true), StructField(value,StringType,true)), [value#26], [key#21, value#22], obj#36: scala.Option, class[userId[0]: bigint, visitedPages[0]: array<struct<eventTime:bigint,pageName:string>>, browser[0]: string, language[0]: string, site[0]: string, apiVersion[0]: string, expirationTimeMillisUtc[0]: bigint, isActive[0]: boolean], Update, true, EventTimeTimeout
      //+- AppendColumns com.waitingforcode.streaming.Application$$$Lambda$984/48042118@18d30e7, interface org.apache.spark.sql.Row, [StructField(key,StringType,true), StructField(value,StringType,true)], createexternalrow(key#21.toString, value#22.toString, StructField(key,StringType,true), StructField(value,StringType,true)), [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false) AS value#26]
      //   +- Project [cast(key#7 as string) AS key#21, cast(value#8 as string) AS value#22]
      //      +- StreamingRelationV2 org.apache.spark.sql.kafka010.KafkaSourceProvider@72b40f87, kafka, Map(checkpointLocation -> /tmp/sessionization-demo-streaming/checkpoint, client.id -> sessionization-demo-streaming, subscribe -> raw_data, kafka.bootstrap.servers -> 160.0.0.20:9092), [key#7, value#8, topic#9, partition#10, offset#11L, timestamp#12, timestampType#13], StreamingRelation DataSource(org.apache.spark.sql.SparkSession@2347b7af,kafka,List(),None,List(),None,Map(checkpointLocation -> /tmp/sessionization-demo-streaming/checkpoint, client.id -> sessionization-demo-streaming, subscribe -> raw_data, kafka.bootstrap.servers -> 160.0.0.20:9092),None), kafka, [key#0, value#1, topic#2, partition#3, offset#4L, timestamp#5, timestampType#6]
      // It fails for COMPLETE MODE AS WELL......
      //Exception in thread "main" org.apache.spark.sql.AnalysisException: Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets;;
      //SerializeFromObject [mapobjects(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), if (isnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true))) null else named_struct(userId, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).userId, site, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).site, true, false), apiVersion, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).apiVersion, true, false), browser, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).browser, true, false), language, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).language, true, false), eventTime, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).eventTime, true, false), timeOnPageMillis, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).timeOnPageMillis, page, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).page, true, false)), unwrapoption(ObjectType(interface scala.collection.Seq), input[0, scala.Option, true]), None) AS value#37]
      //+- FlatMapGroupsWithState org.apache.spark.sql.KeyValueGroupedDataset$$Lambda$1051/1411206559@451816fd, cast(value#26 as string).toString, createexternalrow(key#21.toString, value#22.toString, StructField(key,StringType,true), StructField(value,StringType,true)), [value#26], [key#21, value#22], obj#36: scala.Option, class[userId[0]: bigint, visitedPages[0]: array<struct<eventTime:bigint,pageName:string>>, browser[0]: string, language[0]: string, site[0]: string, apiVersion[0]: string, expirationTimeMillisUtc[0]: bigint, isActive[0]: boolean], Update, true, EventTimeTimeout
      //   +- AppendColumns com.waitingforcode.streaming.Application$$$Lambda$985/686688828@56e5c8fb, interface org.apache.spark.sql.Row, [StructField(key,StringType,true), StructField(value,StringType,true)], createexternalrow(key#21.toString, value#22.toString, StructField(key,StringType,true), StructField(value,StringType,true)), [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false) AS value#26]
      //      +- Project [cast(key#7 as string) AS key#21, cast(value#8 as string) AS value#22]
      //         +- StreamingRelationV2 org.apache.spark.sql.kafka010.KafkaSourceProvider@45cd8607, kafka, Map(checkpointLocation -> /tmp/sessionization-demo-streaming/checkpoint, client.id -> sessionization-demo-streaming, subscribe -> raw_data, kafka.bootstrap.servers -> 160.0.0.20:9092), [key#7, value#8, topic#9, partition#10, offset#11L, timestamp#12, timestampType#13], StreamingRelation DataSource(org.apache.spark.sql.SparkSession@5dc7841c,kafka,List(),None,List(),None,Map(checkpointLocation -> /tmp/sessionization-demo-streaming/checkpoint, client.id -> sessionization-demo-streaming, subscribe -> raw_data, kafka.bootstrap.servers -> 160.0.0.20:9092),None), kafka, [key#0, value#1, topic#2, partition#3, offset#4L, timestamp#5, timestampType#6]
      // TODO: so I suppose I should also talk about output modes :)
      // TODO: I suppose also that I should write a post about it
      .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(Mapping.mapStreamingLogsToSessions(sessionTimeout))
      // ^------ I tried with visitedPages[0] but it didn't work. TODO: does it mean that the column must be specified
      //                                                                statically?


    val writeQuery = query.writeStream.outputMode(OutputMode.Update())
      .foreachBatch(BatchWriter.writeBatch(outputDir, sparkSession) _).start()

    writeQuery.awaitTermination()
  }

}

object BatchWriter {

  def writeBatch(outputDir: String, sparkSession: SparkSession)(batchDataset: Dataset[SessionIntermediaryState], batchId: Long) = {
    import sparkSession.implicits._
    // TODO ^--------- maybe it's better to put it as an implicit and use everywhere without passing in in parameters?
    batchDataset.filter(state => !state.isActive)
      .flatMap(state => state.toSessionOutputState)
      // TODO: write a blog post about encoders, their internal implementation
      // otherwise it fails with Error:(69, 11) Unable to find encoder for type Seq[com.waitingforcode.core.SessionOutput]. An implicit Encoder[Seq[com.waitingforcode.core.SessionOutput]] is needed to store Seq[com.waitingforcode.core.SessionOutput] instances in a Dataset. Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases.
      //      .map(state => state.toSessionOutputState)
      .withColumn("year", functions.year(functions.col("eventTime")))
      .withColumn("month", functions.month(functions.col("eventTime")))
      .withColumn("day", functions.dayofmonth(functions.col("eventTime")))
      .withColumn("hour", functions.hour(functions.col("eventTime")))
      .write
      // TODO: add a suffixed partition column to avoid uneven distribution?
      .partitionBy("year", "month", "day", "hour")
      .mode(SaveMode.Append) // TODO: different with batch mode because the sematic is different too
      .json(outputDir)
  }

}