package com.waitingforcode.batch

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import com.waitingforcode.core.{BatchWriter, Visit}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Application {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Sessionization-demo: batch approach").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val inputDir = args(0)
    val previousSessionsDir = if (args(1).isEmpty) None else Some(args(1))
    val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH")
    val jobExecutionTime = LocalDateTime.parse(args(2), dateTimeFormat)
    val outputDir = args(3)
    val partitionDir = jobExecutionTime.format(DateTimeFormatter.ofPattern("yyyy/MM/dd/HH"))

    val previousSessions = DataLoader.loadPreviousWindowSessions(sparkSession, previousSessionsDir)
    val inputLogs = sparkSession.read.schema(Visit.Schema).json(inputDir)

    val windowUpperBound = jobExecutionTime.withMinute(59).withSecond(59).toInstant(ZoneOffset.UTC).toEpochMilli
    val joinedData = previousSessions.join(inputLogs,
      inputLogs("user_id") === previousSessions("userId"), "fullouter")
      .groupByKey(log => SessionGeneration.resolveGroupByKey(log))
      .flatMapGroups(SessionGeneration.generate(TimeUnit.MINUTES.toMillis(5), windowUpperBound))

    joinedData.cache()

    // I chosen to write only active sessions but you could also keep all of them for debugging
    joinedData.filter("isActive = true").write.mode(SaveMode.Overwrite)
      .json(s"${outputDir}/open-sessions/${partitionDir}")

    // Of course, you can keep it in a separate JOB but the goal
    // If you keep everything in place, you probably gain a little bit performance because you will avoid
    // to read data from disk
    // coalesce --> can be useful to store data that will be loaded to other stores
    BatchWriter.writeDataset(joinedData, s"${outputDir}/sessions-output/${partitionDir}")
  }

}
