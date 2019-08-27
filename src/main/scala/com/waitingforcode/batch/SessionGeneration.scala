package com.waitingforcode.batch

import com.waitingforcode.core.{InputLogMapper, SessionIntermediaryState}
import org.apache.spark.sql.Row

import scala.collection.mutable

object SessionGeneration {

  def resolveGroupByKey(log: Row): Long = {
    if (SessionIntermediaryState.Mapper.language(log) != null) {
      SessionIntermediaryState.Mapper.userId(log)
    } else {
      InputLogMapper.userId(log)
    }
  }

  def generate(inactivityDurationMs: Long, windowUpperBoundMs: Long)(userId: Long, logs: Iterator[Row]): Seq[SessionIntermediaryState] = {
    val materializedLogs = logs.toSeq
    val firstLog = materializedLogs.head

    val sessions = (Option(InputLogMapper.eventTimeString(firstLog)), Option(SessionIntermediaryState.Mapper.userId(firstLog))) match {
      case (Some(_), Some(_)) => generateRestoredSessionWithNewLogs(dedupedAndSortedLogs(materializedLogs), inactivityDurationMs, Some(firstLog))
      case (None, Some(_)) => generateRestoredSessionWithoutNewLogs(materializedLogs, windowUpperBoundMs)
      case (Some(_), None) => generateRestoredSessionWithNewLogs(dedupedAndSortedLogs(materializedLogs), inactivityDurationMs, None)
      case (None, None) => throw new IllegalStateException("Session generation when there is not input nor previous " +
        "session logs should never happen")
    }
    // TODO: must to deal here with expiration datetime because the logs may be written unordered and we don't
    // want to revoke the sessions. Instead it's easier to wait and since we're doing batch, I suppose that latency
    // is not the first concern !
    sessions
  }

  // TODO: expirationTimeMillisUtc is a kind of fake watermark where we allow the logs to arrive at late
  //       in batch
  private def generateRestoredSessionWithNewLogs(logs: Seq[Row], sessionTimeoutMs: Long, previousSession: Option[Row]): Seq[SessionIntermediaryState] = {
    val generatedSessions = new mutable.ListBuffer[SessionIntermediaryState]()
    var currentSession = previousSession.map(log => SessionIntermediaryState.restoreFromRow(log))
    // TODO: during the talk say that it could also be solved with plain SQL joins but I didn't had enough
    // time to prove that and after all, the code gives a little bit more flexibility for that
    for (log <- logs) {
      if (currentSession.isEmpty) {
        currentSession = Some(SessionIntermediaryState.createNew(Iterator(log), sessionTimeoutMs)) // TODO: change too
      } else if (currentSession.get.expirationTimeMillisUtc >= InputLogMapper.eventTime(log)) {
        currentSession = Some(currentSession.get.updateWithNewLogs(Iterator(log), sessionTimeoutMs)) // TODO: change
      } else if (currentSession.get.expirationTimeMillisUtc < InputLogMapper.eventTime(log)) {
        currentSession.foreach(sessionState => generatedSessions.append(sessionState.expire))
        currentSession = Some(SessionIntermediaryState.createNew(Iterator(log), sessionTimeoutMs)) // TODO: change too
      }
    }
    currentSession.foreach(sessionState => generatedSessions.append(sessionState))
    generatedSessions

  }

  private def generateRestoredSessionWithoutNewLogs(logs: Seq[Row], windowUpperBoundMs: Long): Seq[SessionIntermediaryState] = {
    val restoredSession = SessionIntermediaryState.restoreFromRow(logs.head)
    if (windowUpperBoundMs > restoredSession.expirationTimeMillisUtc) {
      Seq(restoredSession.expire)
    } else {
      Seq(restoredSession)
    }
  }

  private[batch] def dedupedAndSortedLogs(logs: Seq[Row]): Seq[Row] = {
    logs.groupBy(row => InputLogMapper.eventTimeString(row))
      .mapValues(rows => rows.head)
      .values
      .toSeq
      .sortBy(row => InputLogMapper.eventTimeString(row))
  }

}
