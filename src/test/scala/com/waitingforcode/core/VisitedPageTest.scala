package com.waitingforcode.core

import com.waitingforcode.test.Mocks
import org.scalatest.{FlatSpec, Matchers}

class VisitedPageTest extends FlatSpec with Matchers {

  "VisitedPage converter" should "construct a VisitedPage from a Row" in {
    val visitedPage = VisitedPage.fromInputLog(Mocks.inputLog("1970-01-01T00:00:25+00:00", "a.html"))

    visitedPage.pageName shouldEqual "a.html"
    visitedPage.eventTime shouldEqual 25000L
  }

}
