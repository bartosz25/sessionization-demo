package com.waitingforcode.batch

import com.waitingforcode.core.SessionIntermediaryState
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataLoader {

  def loadPreviousWindowSessions(sparkSession: SparkSession, previousSessionsDir: Option[String]): DataFrame = {
    previousSessionsDir.map(dir => sparkSession.read.schema(SessionIntermediaryState.Schema)
      .json(dir).filter("isActive = true"))
      .getOrElse(sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], SessionIntermediaryState.Schema))
  }

}
