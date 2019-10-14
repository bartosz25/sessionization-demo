package com.waitingforcode.experiments.memory

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import com.waitingforcode.core.{BatchWriter, Visit}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ApplicationDataset {

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val sparkSession = SparkSession.builder()
      .appName("Sessionization-demo: dataset approach").master("local[*]").getOrCreate()
    import sparkSession.implicits._

    val inputDir = args(0)
    val previousSessionsDir = if (args(1).isEmpty) None else Some(args(1))
    val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH")
    val jobExecutionTime = LocalDateTime.parse(args(2), dateTimeFormat)
    val partitionDir = jobExecutionTime.format(DateTimeFormatter.ofPattern("yyyy/MM/dd/HH"))

    val previousSessions = DataLoader.loadPreviousWindowSessions(sparkSession, previousSessionsDir)
    val sessionsInWindow = sparkSession.read.schema(Visit.Schema).json(inputDir).as[InputLog]

    val windowUpperBound = 1000L // TODO: resolve from the input parameter
    val joinedData = previousSessions.join(sessionsInWindow,
        sessionsInWindow("user_id") === previousSessions("userId"), "fullouter")
      .as[JoinedLog]
      .groupByKey(log => log.groupByKey)
      .flatMapGroups(SessionGeneration.generate(TimeUnit.SECONDS.toMillis(40), windowUpperBound))


    joinedData.cache()
    joinedData.write.mode(SaveMode.Overwrite).json("/tmp/test-windows-dataset")

    BatchWriter.writeDataset(joinedData, partitionDir)
    val end = System.currentTimeMillis()
    println(s"Executed withing ${end - start} ms")
  }

}
