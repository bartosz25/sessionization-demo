package com.waitingforcode.batch

import com.waitingforcode.core.SessionIntermediaryState
import org.apache.spark.sql.Row

object Mapping {

  def buildSessionsFromLogs(userId: Long, logs: Iterator[Row]): SessionIntermediaryState = {
    null
  }

}
