package com.waitingforcode.batch

import com.waitingforcode.core.VisitedPage
import com.waitingforcode.test.{Mocks, SessionIntermediaryStateAssertions}
import org.scalatest.{FlatSpec, Matchers}

class SessionGenerationPreviousSessionNoInputLogsTest extends FlatSpec with Matchers {

  "previous session" should "expire because of the ttl lower than the upper window boundary" in {
    val visitedPages = Seq(
      VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html")
    )
    val sessionToRestore = Mocks.session(
      userId = 30L, expirationTimeMillisUtc = 10000L, visitedPages = visitedPages
    )
    val joinedLog1 = Mocks.inputLogsWithSession(
      None, Some(sessionToRestore)
    )
    val inactivityDuration = 10000L

    val sessions = SessionGeneration.generate(inactivityDuration, 60000L)(3L,
      Iterator(joinedLog1))

    sessions should have size 1
    new SessionIntermediaryStateAssertions(sessions(0))
      .expectedPages(Seq(VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html")))
      .expiringAt(10000L)
      .expired()
      .validate()
  }

  "previous session" should "be extended when the ttl is greater than the upper window boundary" in {
    val visitedPages = Seq(
      VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html")
    )
    val sessionToRestore = Mocks.session(
      // 70000 - after the processing window, so doesn't expire
      userId = 30L, expirationTimeMillisUtc = 70000L, visitedPages = visitedPages
    )
    val joinedLog1 = Mocks.inputLogsWithSession(
      None, Some(sessionToRestore)
    )
    val inactivityDuration = 100000L

    val sessions = SessionGeneration.generate(inactivityDuration, 60000L)(3L,
      Iterator(joinedLog1))

    sessions should have size 1
    new SessionIntermediaryStateAssertions(sessions(0))
      .expectedPages(Seq(VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html")))
      .expiringAt(70000L)
      .validate()
  }

}
