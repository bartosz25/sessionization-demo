package com.waitingforcode.test

import java.sql.Timestamp
import java.time.ZonedDateTime

import com.waitingforcode.core.{SessionIntermediaryState, VisitedPage}
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.GroupState
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

object Mocks {

  def inputLog(eventTime: String, page: String = "test.html", sourceSite: String = "http://",
               apiVersion: String = "1.0", language: String = "en", userId: Long = 30L, browser: String = "Firefox"): Row = {
    val timestamp = ZonedDateTime.parse(eventTime).toInstant.toEpochMilli
    val mockedRow = Mockito.mock(classOf[Row])
    Mockito.when(mockedRow.getAs[Timestamp]("event_time")).thenReturn(new Timestamp(timestamp))
    Mockito.when(mockedRow.getAs[Long]("user_id")).thenReturn(userId)
    val pageRow = Mockito.mock(classOf[Row])
    Mockito.when(mockedRow.getAs[Row]("page")).thenReturn(pageRow)
    Mockito.when(pageRow.getAs[String]("current")).thenReturn(page)
    val technicalRow = Mockito.mock(classOf[Row])
    Mockito.when(mockedRow.getAs[Row]("technical")).thenReturn(technicalRow)
    Mockito.when(technicalRow.getAs[String]("browser")).thenReturn(browser)
    Mockito.when(technicalRow.getAs[String]("lang")).thenReturn(language)
    val sourceRow = Mockito.mock(classOf[Row])
    Mockito.when(mockedRow.getAs[Row]("source")).thenReturn(sourceRow)
    Mockito.when(sourceRow.getAs[String]("site")).thenReturn(sourceSite)
    Mockito.when(sourceRow.getAs[String]("api_version")).thenReturn(apiVersion)
    mockedRow
  }

  def session(userId: Long, expirationTimeMillisUtc: Long, visitedPages: Seq[VisitedPage], language: String = "en",
              browser: String = "Firefox",  site: String = "http://", apiVersion: String = "1.0",
              isActive: Boolean = true): Row = {
    val session = Mockito.mock(classOf[Row])
    val mockToValueMapping = Map("userId" -> userId, "browser" -> browser, "language" -> language, "site" -> site,
      "apiVersion" -> apiVersion, "expirationTimeMillisUtc" -> expirationTimeMillisUtc)
    mockToValueMapping.foreach {
      case (key, value) => {
        Mockito.when(session.getAs(key)).thenReturn(value)
      }
    }
    val visitedPagesAsRows = visitedPages.map(page => {
      visitedPage(page.eventTime, page.pageName)
    })
    Mockito.when(session.getAs("visitedPages")).thenReturn(visitedPagesAsRows)
    session
  }

  private def visitedPage(eventTime: Long, pageName: String) = {
    val page = Mockito.mock(classOf[Row])
    Mockito.when(page.getAs[Long]("eventTime")).thenReturn(eventTime)
    Mockito.when(page.getAs[String]("pageName")).thenReturn(pageName)
    page
  }

  def inputLogsWithSession(inputLog: Option[Row], session: Option[Row]) = {
    val joinedRow = Mockito.mock(classOf[Row], new Answer[Object]() {
      override def answer(invocation: InvocationOnMock): Object = {
        val argumentName = invocation.getArgumentAt(0, classOf[String])
        val valueCandidate = Option(inputLog.map(log => log.getAs[Object](argumentName)).getOrElse(null))
        valueCandidate.getOrElse({
          session.map(row => row.getAs[Object](argumentName)).getOrElse(null)
        })
      }
    })
    joinedRow
  }

  def groupState(intermediaryState: SessionIntermediaryState,
                 hasTimedOut: Boolean): GroupState[SessionIntermediaryState] = {
    val state = Mockito.mock(classOf[GroupState[SessionIntermediaryState]])
    Mockito.when(state.get).thenReturn(intermediaryState)
    Mockito.when(state.hasTimedOut).thenReturn(hasTimedOut)
    Mockito.when(state.getOption).thenReturn(Option(intermediaryState))
    state
  }
}
