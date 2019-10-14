package com.waitingforcode.core

import org.apache.spark.sql.{Dataset, SaveMode}

object BatchWriter {

  def writeDataset(sessions: Dataset[SessionIntermediaryState], outputDir: String): Unit = {
    import sessions.sparkSession.implicits._
    sessions
      .filter(state => !state.isActive)
      .flatMap(state => state.toSessionOutputState)
      .coalesce(50)
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .json(outputDir)
  }

}
