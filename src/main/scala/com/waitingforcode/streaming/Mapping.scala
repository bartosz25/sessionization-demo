package com.waitingforcode.streaming

import com.waitingforcode.core.SessionIntermediaryState
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.GroupState

object Mapping {

  def mapStreamingLogsToSessions(timeoutDurationMs: Long)(key: Long, logs: Iterator[Row],
                                 currentState: GroupState[SessionIntermediaryState]): SessionIntermediaryState = {
    if (currentState.hasTimedOut) {
      val expiredState = currentState.get.expire
      currentState.remove()
      expiredState
    } else {
      val newState = currentState.getOption.map(state => state.updateWithNewLogs(logs, timeoutDurationMs))
        .getOrElse(SessionIntermediaryState.createNew(logs, timeoutDurationMs))
      currentState.update(newState)
      currentState.setTimeoutTimestamp(currentState.getCurrentWatermarkMs() + timeoutDurationMs)
      currentState.get
    }
  }

  def mapStreamingLogsToSessionsProcessingTime(timeoutDurationMs: Long)(key: Long, logs: Iterator[Row],
                                                          currentState: GroupState[SessionIntermediaryState]): SessionIntermediaryState = {
    if (currentState.hasTimedOut) {
      val expiredState = currentState.get.expire
      println(s"State ${expiredState} expired!")
      currentState.remove()
      expiredState
    } else {
      val newState = currentState.getOption.map(state => state.updateWithNewLogs(logs, timeoutDurationMs))
        .getOrElse(SessionIntermediaryState.createNew(logs, timeoutDurationMs))
      currentState.update(newState)
      currentState.setTimeoutDuration(timeoutDurationMs)
      println(s"Creating state for ${key} and ${currentState.getCurrentProcessingTimeMs()}")
      currentState.get
    }
  }

}
