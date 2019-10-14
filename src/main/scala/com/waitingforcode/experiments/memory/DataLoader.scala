package com.waitingforcode.experiments.memory

import com.waitingforcode.core.SessionIntermediaryState
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object DataLoader {

  def loadPreviousWindowSessions(sparkSession: SparkSession,
                                 previousSessionsDir: Option[String]): Dataset[SessionIntermediaryState] = {
    import sparkSession.implicits._
    previousSessionsDir.map(dir => sparkSession.read.schema(SessionIntermediaryState.Schema)
      .json(dir).filter("isActive = true"))
      .getOrElse(sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], SessionIntermediaryState.Schema))
      .as[SessionIntermediaryState]
  }

}
