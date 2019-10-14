package com.waitingforcode.core

import com.waitingforcode.test.Mocks
import org.scalatest.{FlatSpec, Matchers}

class SessionIntermediaryStateLifecycleTest extends FlatSpec with Matchers {

  private val defaultSession = SessionIntermediaryState(
    userId = 1L, visitedPages = Seq(VisitedPage(0L, "1.html")), browser = "Firefox", language = "fr",
    site = "google.com", apiVersion = "v2", expirationTimeMillisUtc = 1000L, isActive = true
  )

  behavior of "SessionIntermediaryState"

  it should "become inactive at expiration" in {
    val expiredSession = defaultSession.expire

    defaultSession.isActive shouldBe true
    expiredSession.isActive shouldBe false
  }

  it should "be updated with new input logs" in {
    val inputLogs = Iterator(
      Mocks.inputLog("1970-01-01T00:00:25+00:00", "a.html"), Mocks.inputLog("1970-01-01T00:00:35+00:00", "b.html")
    )

    val updatedState = defaultSession.updateWithNewLogs(inputLogs, 5000L)

    // 35000L ==> max event time
    updatedState.expirationTimeMillisUtc shouldEqual 35000L + 5000L
    updatedState.visitedPages should have size 3
    updatedState.visitedPages should contain allElementsOf(
      defaultSession.visitedPages ++ Seq(VisitedPage(25000L, "a.html"), VisitedPage(35000L, "b.html"))
    )
  }

  "3 input logs defined in disorder" should "create a new session" in {
    val inputLogs = Iterator(
      // other browser just to assert that the data of the first log from the iterator is taken for the visit metadata
      Mocks.inputLog("1970-01-01T00:00:55+00:00", "c.html", browser = "Chrome"),
      Mocks.inputLog("1970-01-01T00:00:25+00:00", "a.html"),
      Mocks.inputLog("1970-01-01T00:00:35+00:00", "b.html")
    )

    val sessionState = SessionIntermediaryState.createNew(inputLogs, 5000L)

    sessionState.visitedPages should have size 3
    sessionState.visitedPages should contain allOf(VisitedPage(25000L, "a.html"),
      VisitedPage(35000L, "b.html"), VisitedPage(55000L, "c.html")
    )
    sessionState.userId shouldEqual 30L
    sessionState.browser shouldEqual "Chrome"
    sessionState.language shouldEqual "en"
    sessionState.site shouldEqual "http://"
    sessionState.apiVersion shouldEqual "1.0"
    sessionState.expirationTimeMillisUtc shouldEqual 60000L
    sessionState.isActive shouldEqual true
  }

  "already existent Row-based session" should "be converted into SessionIntermediaryState" in {
    val visitedPages = Seq(
      VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html")
    )
    val sessionToRestore = Mocks.session(
      userId = 30L, expirationTimeMillisUtc = 10000L, visitedPages = visitedPages
    )

    val restoredSession = SessionIntermediaryState.restoreFromRow(sessionToRestore)

    restoredSession.userId shouldEqual 30L
    restoredSession.visitedPages should have size 2
    restoredSession.visitedPages should contain allElementsOf visitedPages
    restoredSession.browser shouldEqual "Firefox"
    restoredSession.language shouldEqual "en"
    restoredSession.site shouldEqual "http://"
    restoredSession.apiVersion shouldEqual "1.0"
    restoredSession.expirationTimeMillisUtc shouldEqual 10000L
    restoredSession.isActive shouldBe true
  }

  "already existent Row-based session" should "be converted into an expired SessionIntermediaryState" in {
    val sessionToRestore = Mocks.session(
      userId = 30L, expirationTimeMillisUtc = 10000L, visitedPages = Seq(
        VisitedPage(3000L, "a.html"), VisitedPage(4000L, "b.html")
      )
    )

    val restoredSession = SessionIntermediaryState.restoreFromRow(sessionToRestore, false)

    restoredSession.isActive shouldBe false
  }
}
