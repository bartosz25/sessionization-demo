package com.waitingforcode.batch

import com.waitingforcode.core.VisitedPage
import com.waitingforcode.test.{Mocks, SessionIntermediaryStateAssertions}
import org.scalatest.{FlatSpec, Matchers}

class SessionGenerationPreviousSessionNewLogsTest extends FlatSpec with Matchers {

  "session from previous window" should "expire when the first log is after session's expiration time" in {
    // we expect to have 2 sessions: one for old, one for new
    val visitedPages = Seq(
      VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html")
    )
    val sessionToRestore = Mocks.session(
      userId = 30L, expirationTimeMillisUtc = 10000L, visitedPages = visitedPages
    )
    val joinedLog1 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:15+00:00", "c.html")), Some(sessionToRestore)
    )
    val joinedLog2 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:25+00:00", "d.html")), Some(sessionToRestore)
    )

    // upperBound = 39 sec, before the expiration time for the last session
    val sessions = SessionGeneration.generate(15000L, 30000L)(30L, Iterator(joinedLog1, joinedLog2))
    val sessionsPerPage = sessions.groupBy(session => session.visitedPages.head.pageName)
      .mapValues(mappedSessions => mappedSessions.head)
    sessions should have size 2
    new SessionIntermediaryStateAssertions(sessionsPerPage("a.html"))
      .expectedPages(Seq(VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html")))
      .expired()
      .expiringAt(10000L)
      .validate()
    new SessionIntermediaryStateAssertions(sessionsPerPage("c.html"))
      .expectedPages(Seq(VisitedPage(15000L, "c.html"), VisitedPage(25000L, "d.html")))
      .expiringAt(40000L)
      .validate()
  }

  "session from previous window" should "include new logs when the first log is before session's expiration time" in {
    // session should also extend timeout!
    val visitedPages = Seq(
      VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html")
    )
    val sessionToRestore = Mocks.session(
      userId = 30L, expirationTimeMillisUtc = 20000L, visitedPages = visitedPages
    )
    val joinedLog1 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:15+00:00", "c.html")), Some(sessionToRestore)
    )
    val joinedLog2 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:25+00:00", "d.html")), Some(sessionToRestore)
    )

    // upperBound = 39 sec, before the expiration time for the last session
    val sessions = SessionGeneration.generate(15000L, 39000L)(30L, Iterator(joinedLog1, joinedLog2))

    sessions should have size 1
    new SessionIntermediaryStateAssertions(sessions.head)
      .expectedPages(Seq(VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html"),
        VisitedPage(15000L, "c.html"), VisitedPage(25000L, "d.html")
      ))
      .expiringAt(40000L)
      .validate()
  }

  "session from previous window" should "expire and generate 3 sessions because of the timeouts" in {
    val visitedPages = Seq(
      VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html")
    )
    val sessionToRestore = Mocks.session(
      userId = 30L, expirationTimeMillisUtc = 10000L, visitedPages = visitedPages
    )
    val joinedLog1 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:15+00:00", "c.html")), Some(sessionToRestore)
    )
    val joinedLog2 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:45+00:00", "d.html")), Some(sessionToRestore)
    )

    val sessions = SessionGeneration.generate(15000L, 60000L)(30L, Iterator(joinedLog1, joinedLog2))

    sessions should have size 3
    val sessionsPerPage = sessions.groupBy(session => session.visitedPages.head.pageName)
      .mapValues(mappedSessions => mappedSessions.head)
    new SessionIntermediaryStateAssertions(sessionsPerPage("a.html"))
      .expectedPages(Seq(VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html")))
      .expired()
      .expiringAt(10000L)
      .validate()
    new SessionIntermediaryStateAssertions(sessionsPerPage("c.html"))
      .expectedPages(Seq(VisitedPage(15000L, "c.html")))
      .expired()
      .expiringAt(30000L)
      .validate()
    new SessionIntermediaryStateAssertions(sessionsPerPage("d.html"))
      .expectedPages(Seq(VisitedPage(45000L, "d.html")))
      .expiringAt(60000L)
      .validate()
  }

  "session from previous window" should "include new logs when the first log is before session's expiration time and " +
    "generate an expired session because of the window's upper bound" in {
    val visitedPages = Seq(
      VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html")
    )
    val sessionToRestore = Mocks.session(
      userId = 30L, expirationTimeMillisUtc = 20000L, visitedPages = visitedPages
    )
    val joinedLog1 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:15+00:00", "c.html")), Some(sessionToRestore)
    )
    val joinedLog2 = Mocks.inputLogsWithSession(
      Some(Mocks.inputLog("1970-01-01T00:00:25+00:00", "d.html")), Some(sessionToRestore)
    )

    // upperBound = 60 sec, so the last session will expire
    val sessions = SessionGeneration.generate(15000L, 60000L)(30L, Iterator(joinedLog1, joinedLog2))

    sessions should have size 1
    new SessionIntermediaryStateAssertions(sessions.head)
      .expectedPages(Seq(VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html"),
        VisitedPage(15000L, "c.html"), VisitedPage(25000L, "d.html")
      ))
      .expired()
      .expiringAt(40000L)
      .validate()
  }
}
