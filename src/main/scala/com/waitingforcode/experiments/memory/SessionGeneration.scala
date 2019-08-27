package com.waitingforcode.experiments.memory

import com.waitingforcode.core.{SessionIntermediaryState, VisitedPage}

import scala.collection.mutable

object SessionGeneration {

  def generate(inactivityDurationMs: Long, windowUpperBoundMs: Long)
              (userId: Long, logs: Iterator[JoinedLog]): Seq[SessionIntermediaryState] = {
    val materializedLogs = logs.toSeq
    val firstLog = materializedLogs.head

    val sessions = (firstLog.event_time, firstLog.userId) match {
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
  private def generateRestoredSessionWithNewLogs(logs: Seq[JoinedLog], sessionTimeoutMs: Long, previousSession: Option[JoinedLog]): Seq[SessionIntermediaryState] = {
    val generatedSessions = new mutable.ListBuffer[SessionIntermediaryState]()
    var currentSession = previousSession.map(log => SessionIntermediaryState2.restoreFromRow(log))
    // TODO: during the talk say that it could also be solved with plain SQL joins but I didn't had enough
    // time to prove that and after all, the code gives a little bit more flexibility for that
    for (log <- logs) {
      if (currentSession.isEmpty) {
        currentSession = Some(SessionIntermediaryState2.createNew(Iterator(log), sessionTimeoutMs)) // TODO: change too
      } else if (currentSession.get.expirationTimeMillisUtc >= log.eventTimeMillis.get) {
        currentSession = Some(updateWithNewLogs(currentSession.get, log, sessionTimeoutMs)) // TODO: change
      } else if (currentSession.get.expirationTimeMillisUtc < log.eventTimeMillis.get) {
        currentSession.foreach(sessionState => generatedSessions.append(sessionState.expire))
        currentSession = Some(SessionIntermediaryState2.createNew(Iterator(log), sessionTimeoutMs)) // TODO: change too
      }
    }
    currentSession.foreach(sessionState => generatedSessions.append(sessionState))
    generatedSessions

  }

  def updateWithNewLogs(sessionIntermediaryState: SessionIntermediaryState, newLog: JoinedLog, timeoutDurationMs: Long): SessionIntermediaryState = {
    val newVisitedPages = Seq(VisitedPage(newLog.eventTimeMillis.get, newLog.page.get.current))

    sessionIntermediaryState.copy(visitedPages = sessionIntermediaryState.visitedPages ++ newVisitedPages,
      expirationTimeMillisUtc = SessionIntermediaryState2.getTimeout(newVisitedPages.last.eventTime, timeoutDurationMs))
  }

  private def generateRestoredSessionWithoutNewLogs(logs: Seq[JoinedLog], windowUpperBoundMs: Long): Seq[SessionIntermediaryState] = {
    val restoredSession = SessionIntermediaryState2.restoreFromRow(logs.head)
    if (windowUpperBoundMs > restoredSession.expirationTimeMillisUtc) {
      Seq(restoredSession.expire)
    } else {
      Seq(restoredSession)
    }
  }

  private[batch] def dedupedAndSortedLogs(logs: Seq[JoinedLog]): Seq[JoinedLog] = {
    logs.groupBy(row => row.event_time)
      .mapValues(rows => rows.head)
      .values
      .toSeq
      .sortBy(row => row.event_time)
  }

}
