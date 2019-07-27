package com.waitingforcode.core

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import com.waitingforcode.core.InputLogMapper.{currentPage, eventTime}
import org.apache.spark.sql.Row

case class SessionIntermediaryState(userId: Long, visitedPages: Seq[VisitedPage],
                                    browser: String, language: String, site: String,
                                    apiVersion: String,
                                    // TODO: I could ignore this property and pass it to the method generating the output
                                    //        for an expired session. But I prefer to keep it here in order to share the
                                    //        same abstraction for batch and streaming. Or maybe it's better to have
                                    //        an Option[Long] here and use it only in batch?
                                    expirationTimeMillisUtc: Long,
                                    isActive: Boolean) {

  def updateWithNewLogs(newLogs: Iterator[Row], timeoutDurationMs: Long): SessionIntermediaryState = {
    val newVisitedPages = SessionIntermediaryState.mapInputLogsToVisitedPages(newLogs.toSeq)

    this.copy(visitedPages = visitedPages ++ newVisitedPages,
      expirationTimeMillisUtc = SessionIntermediaryState.getTimeout(newVisitedPages.last.eventTime, timeoutDurationMs))
  }

  def toSessionOutputState: Iterator[SessionOutput] = {
    visitedPages.tails.collect {
      case Seq(firstVisit, secondVisit, _*) => firstVisit.toSessionOutput(this, Some(secondVisit))
      case Seq(firstVisit) => firstVisit.toSessionOutput(this, None)
    }
  }

}

case class VisitedPage(eventTime: Long, pageName: String) {

  def toSessionOutput(session: SessionIntermediaryState, nextVisit: Option[VisitedPage]): SessionOutput = {
    val duration = nextVisit.map(visit => visit.eventTime - eventTime)
        .getOrElse(session.expirationTimeMillisUtc - eventTime)
    val eventTimeAsDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(eventTime), ZoneOffset.UTC)

    SessionOutput(
      userId = session.userId, site = session.site, apiVersion = session.apiVersion,
      browser = session.browser, language = session.language,
      eventTime = VisitedPage.Formatter.format(eventTimeAsDateTime),
      timeOnPageMillis = duration, page = pageName
    )
  }

}

object VisitedPage {

  private val Formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  def fromInputLog(log: Row): VisitedPage = {
    VisitedPage(eventTime(log), currentPage(log))
  }

}

object SessionIntermediaryState {

  object Mapper {
    def userId(session: Row) = session.getAs[Long]("userId")
  }

  def createNew(logs: Iterator[Row], timeoutDurationMs: Long): SessionIntermediaryState = {
    val materializedLogs = logs.toSeq
    val visitedPages = SessionIntermediaryState.mapInputLogsToVisitedPages(materializedLogs)
    val headLog = materializedLogs.head

    SessionIntermediaryState(userId = InputLogMapper.userId(headLog), visitedPages = visitedPages, isActive = true,
      browser = InputLogMapper.browser(headLog), language = InputLogMapper.language(headLog),
      site = InputLogMapper.site(headLog),
      apiVersion = InputLogMapper.apiVersion(headLog),
      expirationTimeMillisUtc = getTimeout(visitedPages.last.eventTime, timeoutDurationMs)
    )
  }

  private def getTimeout(eventTime: Long, timeoutDurationMs: Long) = eventTime + timeoutDurationMs

  private[core] def mapInputLogsToVisitedPages(logs: Seq[Row]): Seq[VisitedPage] =
    logs.map(log => VisitedPage.fromInputLog(log)).toSeq.sortBy(visitedPage => visitedPage.eventTime)

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
