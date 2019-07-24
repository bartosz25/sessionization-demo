package com.waitingforcode.core

import com.waitingforcode.core.InputLogMapper.{currentPage, eventTime}
import org.apache.spark.sql.Row

case class SessionIntermediaryState(userId: Long, visitedPages: Iterator[VisitedPage]) {

  def updateWithNewLogs(newLogs: Iterator[Row]): SessionIntermediaryState = {
    val newVisitedPages = SessionIntermediaryState.mapInputLogsToVisitedPages(newLogs)
    this.copy(visitedPages = visitedPages ++ newVisitedPages)
  }

}

case class VisitedPage(eventTime: Long, pageName: String)
object VisitedPage {

  def fromInputLog(log: Row): VisitedPage = {
    VisitedPage(eventTime(log), currentPage(log))
  }

}

object SessionIntermediaryState {

  def createNew(logs: Iterator[Row]): SessionIntermediaryState = {
    val headLog = logs.next()
    val allLogs = Iterator(VisitedPage.fromInputLog(headLog)) ++
      SessionIntermediaryState.mapInputLogsToVisitedPages(logs)

    SessionIntermediaryState(userId = InputLogMapper.userId(headLog), visitedPages = allLogs)
  }

  private[core] def mapInputLogsToVisitedPages(logs: Iterator[Row]): Iterator[VisitedPage] =
    logs.map(log => VisitedPage.fromInputLog(log))

}