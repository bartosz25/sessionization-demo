package com.waitingforcode.core

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import com.waitingforcode.core.InputLogMapper.{currentPage, eventTime}
import org.apache.spark.sql.Row

import scala.util.hashing.MurmurHash3

case class SessionIntermediaryState(userId: Long, visitedPages: Seq[VisitedPage],
                                    browser: String, language: String, site: String,
                                    apiVersion: String,
                                    // TODO: I could ignore this property and pass it to the method generating the output
                                    //        for an expired session. But I prefer to keep it here in order to share the
                                    //        same abstraction for batch and streaming. Or maybe it's better to have
                                    //        an Option[Long] here and use it only in batch?
                                    expirationTimeMillisUtc: Long,
                                    isActive: Boolean) {

  lazy val id = {
    val firstVisitedPage = visitedPages.head
    val idKey = s"${firstVisitedPage.eventTime}-${firstVisitedPage.eventTime}-${userId}"
    MurmurHash3.stringHash(idKey)
  }

  def updateWithNewLogs(newLogs: Iterator[Row], timeoutDurationMs: Long): SessionIntermediaryState = {
    val newVisitedPages = SessionIntermediaryState.mapInputLogsToVisitedPages(newLogs.toSeq)

    this.copy(visitedPages = visitedPages ++ newVisitedPages,
      expirationTimeMillisUtc = SessionIntermediaryState.getTimeout(newVisitedPages.last.eventTime, timeoutDurationMs))
  }

  def expire = this.copy(isActive = false)

  def toSessionOutputState: Seq[SessionOutput] = {
    visitedPages.tails.collect {
      case Seq(firstVisit, secondVisit, _*) => firstVisit.toSessionOutput(this, Some(secondVisit))
      case Seq(firstVisit) => firstVisit.toSessionOutput(this, None)
    }.toSeq
  }

}

case class VisitedPage(eventTime: Long, pageName: String) {

  def toSessionOutput(session: SessionIntermediaryState, nextVisit: Option[VisitedPage]): SessionOutput = {
    val duration = nextVisit.map(visit => visit.eventTime - eventTime)
        .getOrElse(session.expirationTimeMillisUtc - eventTime)
    val eventTimeAsDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(eventTime), ZoneOffset.UTC)

    SessionOutput(
      id = session.id,
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
    def browser(session: Row): String = session.getAs[String]("browser")
    def language(session: Row): String = session.getAs[String]("language")
    def site(session: Row): String = session.getAs[String]("site")
    def apiVersion(session: Row): String = session.getAs[String]("apiVersion")
    def expirationTimeMillisUtc(session: Row): Long = session.getAs[Long]("expirationTimeMillisUtc")
    def visitedPages(session: Row): Seq[VisitedPage] = {
      Option(session.getAs[Seq[Row]]("visitedPages")).map(pages => pages.map(row => VisitedPage(
        row.getAs[Long]("eventTime"), row.getAs[String]("pageName")
      ))).getOrElse(Seq.empty)
    }
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

  def restoreFromRow(row: Row, isActive: Boolean = true) = {
    SessionIntermediaryState(
      userId = Mapper.userId(row), browser = Mapper.browser(row), language = Mapper.language(row),
      site = Mapper.site(row), apiVersion = Mapper.apiVersion(row),
      expirationTimeMillisUtc = Mapper.expirationTimeMillisUtc(row),
      isActive = isActive, visitedPages = Mapper.visitedPages(row)
    )
  }

  private def getTimeout(eventTime: Long, timeoutDurationMs: Long) = eventTime + timeoutDurationMs

  private[core] def mapInputLogsToVisitedPages(logs: Seq[Row]): Seq[VisitedPage] =
    logs.map(log => VisitedPage.fromInputLog(log)).sortBy(visitedPage => visitedPage.eventTime)

  val Schema = new StructBuilder()
    .withRequiredFields(Map(
      "userId" -> fields.long,
      "visitedPages" -> fields.array(
        fields.newStruct.withRequiredFields(
          Map("eventTime" -> fields.long, "pageName" -> fields.string)
        ).buildSchema, nullableContent = false
      ),
      "browser" -> fields.string, "language" -> fields.string, "site" -> fields.string, "apiVersion" -> fields.string,
      "expirationTimeMillisUtc" -> fields.long,
      "isActive" -> fields.boolean
    )).buildSchema

}
