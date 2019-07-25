package com.waitingforcode.core

import org.scalatest.{FlatSpec, Matchers}

class SessionIntermediaryStateTest extends FlatSpec with Matchers {

  private val defaultSession = SessionIntermediaryState(
    userId = 1L, visitId = 100L, visitedPages = Iterator.empty, browser = "Firefox", language = "fr",
    source = "google.com", apiVersion = "v2", expirationTimeMillisUtc = 1000L, isActive = true
  )
  private val defaultSessionOutput = SessionOutput(
    userId = 1L, visitId = 100L, browser = "Firefox", language = "fr",
    source = "google.com", apiVersion = "v2", eventTime = "TODO: reformat me", timeOnPageMillis = 0L, page = ""
  )

  behavior of "SessionIntermediaryState"

  it should "convert to output with only 1 visit" in {
    val session = defaultSession.copy(visitedPages = Iterator(VisitedPage(500L, "page1")))

    val output = session.toSessionOutputState.toSeq

    output should have size 1
    output should contain only (defaultSessionOutput.copy(timeOnPageMillis = 500L, page = "page1"))
  }

  it should "convert to output with 2 visits" in {
    val session = defaultSession.copy(visitedPages = Iterator(VisitedPage(500L, "page1"), VisitedPage(800L, "page2")))

    val output = session.toSessionOutputState.toSeq

    output should have size 2
    output should contain allOf (defaultSessionOutput.copy(timeOnPageMillis = 300L, page = "page1"),
      defaultSessionOutput.copy(timeOnPageMillis = 200L, page = "page2"))
  }

  it should "convert to output with 3 visits" in {
    val session = defaultSession.copy(visitedPages = Iterator(VisitedPage(500L, "page1"), VisitedPage(800L, "page2"),
      VisitedPage(850L, "page3")))

    val output = session.toSessionOutputState.toSeq

    output should have size 3
    output should contain allOf (defaultSessionOutput.copy(timeOnPageMillis = 300L, page = "page1"),
      defaultSessionOutput.copy(timeOnPageMillis = 50L, page = "page2"),
      defaultSessionOutput.copy(timeOnPageMillis = 150L, page = "page3"))
  }

}
