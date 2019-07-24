package com.waitingforcode.batch

import com.waitingforcode.core.{InputLogMapper, Visit}
import org.apache.spark.sql.SparkSession

object Application {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Sessionization-demo: batch approach").master("local[*]").getOrCreate()
    // some orchstration & ops effort because you have to configure your triggering to read appropriate
    // partitions

    val inputDir = args(0)

    // TODO: add fullOuter join to keep unactive sessions

    val sessionsInWindow = sparkSession.read.schema(Visit.Schema).json(inputDir)
      // TODO: talk about a difference between groupBy and groupByKey
      .groupByKey(log => InputLogMapper.userId(log))
      .mapGroups(Mapping.buildSessionsFromLogs)

    sessionsInWindow.write.partitionBy("col1").json("/tmp/test")
  }

}
