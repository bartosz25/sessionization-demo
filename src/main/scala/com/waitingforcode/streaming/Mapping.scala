package com.waitingforcode.streaming

import com.waitingforcode.core.{SessionIntermediaryState, SessionOutput}
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.GroupState

object Mapping {

  def mapStreamingLogsToSessions(key: String, logs: Iterator[Row],
                                 currentState: GroupState[SessionIntermediaryState]): Option[Seq[SessionOutput]] = {
    if (currentState.hasTimedOut) {
      Some(currentState.get.toSessionOutputState)
    } else {
      val newState = currentState.getOption.map(state => state.updateWithNewLogs(logs))
        .getOrElse(SessionIntermediaryState.createNew(logs))
      currentState.update(newState)
      // TODO: handle state expiration here
      None
    }
  }

}
