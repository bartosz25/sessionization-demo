package com.waitingforcode.streaming

import com.waitingforcode.core.{SessionIntermediaryState, SessionOutput}
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.GroupState

object Mapping {

  def mapStreamingLogsToSessions(timeoutDurationMs: Long)(key: String, logs: Iterator[Row],
                                 currentState: GroupState[SessionIntermediaryState]): Option[Iterator[SessionOutput]] = {
    if (currentState.hasTimedOut) {
      Some(currentState.get.toSessionOutputState)
    } else {
      val newState = currentState.getOption.map(state => state.updateWithNewLogs(logs, timeoutDurationMs))
        .getOrElse(SessionIntermediaryState.createNew(logs, timeoutDurationMs))
      currentState.update(newState)
      // TODO: talk about different possibilities to configure the timeout; here we're using event-time based but
      //       you can also use processing time-based;
      // TODO: talk also about what it involves
      currentState.setTimeoutTimestamp(newState.expirationTimeMillisUtc)
      None
    }
  }

}
