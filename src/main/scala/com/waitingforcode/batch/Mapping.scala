package com.waitingforcode.batch

import com.waitingforcode.core.SessionIntermediaryState
import org.apache.spark.sql.Row

object Mapping {

  def buildSessionsFromLogs(userId: Long, logs: Iterator[Row]): SessionIntermediaryState = {
    // TODO: must to deal here with expiration datetime because the logs may be written unordered and we don't
    // want to revoke the sessions. Instead it's easier to wait and since we're doing batch, I suppose that latency
    // is not the first concern !
    null
  }

}
