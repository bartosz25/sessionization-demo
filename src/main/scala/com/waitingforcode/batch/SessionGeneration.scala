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

    val sessions = (Option(InputLogMapper.eventTimeTimestamp(firstLog)), Option(SessionIntermediaryState.Mapper.apiVersion(firstLog))) match {
      case (Some(_), Some(_)) => generateRestoredSessionWithNewLogs(dedupedAndSortedLogs(materializedLogs), inactivityDurationMs, Some(firstLog))
      case (None, Some(_)) => generateRestoredSessionWithoutNewLogs(materializedLogs, windowUpperBoundMs)
      case (Some(_), None) => generateRestoredSessionWithNewLogs(dedupedAndSortedLogs(materializedLogs), inactivityDurationMs, None)
      case (None, None) => throw new IllegalStateException("Session generation when there is not input nor previous " +
        "session logs should never happen")
    }
    sessions
  }

  private def generateRestoredSessionWithNewLogs(logs: Seq[Row], sessionTimeoutMs: Long, previousSession: Option[Row]): Seq[SessionIntermediaryState] = {
    val generatedSessions = new mutable.ListBuffer[SessionIntermediaryState]()
    var currentSession = previousSession.map(log => SessionIntermediaryState.restoreFromRow(log))
    for (log <- logs) {
      if (currentSession.isEmpty) {
        currentSession = Some(SessionIntermediaryState.createNew(Iterator(log), sessionTimeoutMs))
      } else if (currentSession.get.expirationTimeMillisUtc >= InputLogMapper.eventTime(log)) {
        currentSession = Some(currentSession.get.updateWithNewLogs(Iterator(log), sessionTimeoutMs))
      } else if (currentSession.get.expirationTimeMillisUtc < InputLogMapper.eventTime(log)) {
        currentSession.foreach(sessionState => generatedSessions.append(sessionState.expire))
        currentSession = Some(SessionIntermediaryState.createNew(Iterator(log), sessionTimeoutMs))
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
    logs.groupBy(row => InputLogMapper.eventTimeTimestamp(row))
      .mapValues(rows => rows.head)
      .values
      .toSeq
      .sortBy(row => InputLogMapper.eventTime(row))
  }

}
