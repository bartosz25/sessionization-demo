package com.waitingforcode.batch

import com.waitingforcode.core.SessionIntermediaryState
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoader {

  def loadPreviousWindowSessions(sparkSession: SparkSession, previousSessionsDir: Option[String]): DataFrame = {
    previousSessionsDir.map(dir => sparkSession.read.schema(SessionIntermediaryState.Schema) // TODO: use different schema
      .json(dir).filter("isActive = true")).getOrElse(sparkSession.emptyDataFrame)
  }

}
