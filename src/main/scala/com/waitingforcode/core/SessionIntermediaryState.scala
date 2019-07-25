package com.waitingforcode.core

import com.waitingforcode.core.InputLogMapper.{currentPage, eventTime}
import org.apache.spark.sql.Row

case class SessionIntermediaryState(userId: Long, visitId: Long, visitedPages: Iterator[VisitedPage],
                                    browser: String, language: String, source: String,
                                    apiVersion: String, expirationTimeMillisUtc: Long,
                                    isActive: Boolean) {

  def updateWithNewLogs(newLogs: Iterator[Row]): SessionIntermediaryState = {
    val newVisitedPages = SessionIntermediaryState.mapInputLogsToVisitedPages(newLogs)
    this.copy(visitedPages = visitedPages ++ newVisitedPages)
  }

  def toSessionOutputState: Iterator[SessionOutput] = {
    visitedPages.toSeq.tails.collect {
      case Seq(firstVisit, secondVisit, _*) => firstVisit.toSessionOutput(this, Some(secondVisit))
      case Seq(firstVisit) => firstVisit.toSessionOutput(this, None)
    }
  }

}

case class VisitedPage(eventTime: Long, pageName: String) {

  def toSessionOutput(session: SessionIntermediaryState, nextVisit: Option[VisitedPage]): SessionOutput = {
    val duration = nextVisit.map(visit => visit.eventTime - eventTime)
        .getOrElse(session.expirationTimeMillisUtc - eventTime)
    SessionOutput(
      userId = session.userId, visitId = session.visitId, source = session.source, apiVersion = session.apiVersion,
      browser = session.browser, language = session.language,
      eventTime = "TODO: reformat me", timeOnPageMillis = duration, page = pageName
    )
  }

}

object VisitedPage {

  def fromInputLog(log: Row): VisitedPage = {
    VisitedPage(eventTime(log), currentPage(log))
  }

}

object SessionIntermediaryState {

  object Mapper {
    def userId(session: Row) = session.getAs[Long]("userId")
  }

  def createNew(logs: Iterator[Row]): SessionIntermediaryState = {
    val headLog = logs.next()
    val allLogs = Iterator(VisitedPage.fromInputLog(headLog)) ++
      SessionIntermediaryState.mapInputLogsToVisitedPages(logs)

    SessionIntermediaryState(userId = InputLogMapper.userId(headLog), visitedPages = allLogs, isActive = true,
      // TODO: handle them correctly
      browser = "Firefox", language = "fr", visitId = 30L,
      source = "google.com", apiVersion = "v2", expirationTimeMillisUtc = 1000L
    )
  }

  private[core] def mapInputLogsToVisitedPages(logs: Iterator[Row]): Iterator[VisitedPage] =
    logs.map(log => VisitedPage.fromInputLog(log))

  val Schema = new StructBuilder()
    .withRequiredFields(Map(
      "userId" -> fields.long,
      "visitedPages" -> fields.array(
        fields.newStruct.withRequiredFields(
          Map("eventTime" -> fields.long, "pageName" -> fields.string)
        ).buildSchema, nullableContent = false
      ),
      "isActive" -> fields.boolean
    )).buildSchema

}
