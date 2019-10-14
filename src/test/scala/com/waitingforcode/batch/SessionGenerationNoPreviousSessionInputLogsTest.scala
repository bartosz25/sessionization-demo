package com.waitingforcode.batch

import com.waitingforcode.core.VisitedPage
import com.waitingforcode.test.{Mocks, SessionIntermediaryStateAssertions}
import org.scalatest.{FlatSpec, Matchers}

class SessionGenerationNoPreviousSessionInputLogsTest extends FlatSpec with Matchers {

  "3 logs within the inactivity period" should "generate 1 session" in {
    val joinedLog1 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:03+00:00", "a.html")), None
    )
    val joinedLog2 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:05+00:00", "b.html")), None
    )
    val joinedLog3 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:15+00:00", "c.html")), None
    )
    val inactivityDuration = 60000L

    val sessions = SessionGeneration.generate(inactivityDuration, 60000L)(3L,
      Iterator(joinedLog1, joinedLog2, joinedLog3))

    sessions should have size 1
    new SessionIntermediaryStateAssertions(sessions(0))
      .expectedPages(Seq(VisitedPage(3000L, "a.html"), VisitedPage(5000L, "b.html"), VisitedPage(15000L, "c.html")))
      .expiringAt(75000L) // because 60000L + 15000L of last event
      .validate()
  }

  "3 logs not within the inactivity period" should "generate 2 sessions because of inactivity time" in {
    val joinedLog1 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:03+00:00", "a.html")), None
    )
    val joinedLog2 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:05+00:00", "b.html")), None
    )
    val joinedLog3 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:15+00:00", "c.html")), None
    )
    val inactivityDuration = 6000L

    // windowUpperBound = 20 secs, so before the last session
    val sessions = SessionGeneration.generate(inactivityDuration, 20000L)(3L,
      Iterator(joinedLog1, joinedLog2, joinedLog3))

    sessions should have size 2
    val sessionsPerPage = sessions.groupBy(session => session.visitedPages.head.pageName)
      .mapValues(mappedSessions => mappedSessions.head)
    new SessionIntermediaryStateAssertions(sessionsPerPage("a.html"))
      .expectedPages(Seq(VisitedPage(3000L, "a.html"), VisitedPage(5000L, "b.html")))
      .expired()
      .expiringAt(11000L)
      .validate()
    new SessionIntermediaryStateAssertions(sessionsPerPage("c.html"))
      .expectedPages(Seq(VisitedPage(15000L, "c.html")))
      .expiringAt(21000L)
      .validate()
  }

  "3 logs the inactivity period but expired before the upper bound" should "generate 1 session" in {
    val joinedLog1 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:03+00:00", "a.html")), None
    )
    val joinedLog2 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:05+00:00", "b.html")), None
    )
    val joinedLog3 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:15+00:00", "c.html")), None
    )
    val inactivityDuration = 6000L

    val sessions = SessionGeneration.generate(inactivityDuration, 60000L)(3L,
      Iterator(joinedLog1, joinedLog2, joinedLog3))

    sessions should have size 2
    val sessionsPerPage = sessions.groupBy(session => session.visitedPages.head.pageName)
      .mapValues(mappedSessions => mappedSessions.head)
    new SessionIntermediaryStateAssertions(sessionsPerPage("a.html"))
      .expectedPages(Seq(VisitedPage(3000L, "a.html"), VisitedPage(5000L, "b.html")))
      .expired()
      .expiringAt(11000L)
      .validate()
    new SessionIntermediaryStateAssertions(sessionsPerPage("c.html"))
      .expectedPages(Seq(VisitedPage(15000L, "c.html")))
      .expired()
      .expiringAt(21000L)
      .validate()
  }
}
