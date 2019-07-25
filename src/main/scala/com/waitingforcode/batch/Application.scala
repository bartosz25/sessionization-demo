package com.waitingforcode.batch

import com.waitingforcode.core.Visit
import org.apache.spark.sql.{SaveMode, SparkSession}

object Application {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Sessionization-demo: batch approach").master("local[*]").getOrCreate()
    // some orchstration & ops effort because you have to configure your triggering to read appropriate
    // partitions
    import sparkSession.implicits._

    val inputDir = args(0)
    val previousSessionsDir = Option(args(1))
    /*
    val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH")
    val executionDateTime = dateTimeFormat.parse(args(2))*/
    // ^ check if we have to use it if partitionBy is enabled

    val previousSessions = DataLoader.loadPreviousWindowSessions(sparkSession, previousSessionsDir)

    val sessionsInWindow = sparkSession.read.schema(Visit.Schema).json(inputDir)

    val joinedData = previousSessions.join(sessionsInWindow,
      sessionsInWindow("userId") === previousSessions("userId"), "fullouter")
    joinedData
      // TODO: talk about a difference between groupBy and groupByKey
      .groupByKey(log => SessionGeneration.resolveGroupByKey(log))
      .mapGroups(Mapping.buildSessionsFromLogs)

    joinedData.cache()

    // TODO: write all intermediary sessions as well
    joinedData.write.mode(SaveMode.Overwrite).json("/tmp/test-windows")

    // Of course, you can keep it in a separate JOB but the goal
    // If you keep everything in place, you probably gain a little bit performance because you will avoid
    // to read data from disk
    // coalesce => see if it's worth adding. It could be useful to store data that will be loaded to other stores
    sessionsInWindow
      .filter("isActive = false") // write only inactive sessions to the output
      .map(x => "TODO: conveert to SessionOutput")
      .coalesce(50).write.partitionBy("col1").mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .json("/tmp/test")
  }

}
