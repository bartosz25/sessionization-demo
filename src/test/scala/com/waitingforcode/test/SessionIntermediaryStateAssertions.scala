package com.waitingforcode.test

import com.waitingforcode.core.{SessionIntermediaryState, VisitedPage}
import org.scalatest.Matchers

class SessionIntermediaryStateAssertions(session: SessionIntermediaryState) extends Matchers {

  private var expectedPages: Seq[VisitedPage] = Seq.empty

  private var browser = "Firefox"
  private var language = "en"
  private var userId = 30L
  private var apiVersion = "1.0"
  private var site = "http://"
  private var isActive = true
  private var expirationTime = 0L

  def expiringAt(expirationTime: Long): SessionIntermediaryStateAssertions = {
    this.expirationTime = expirationTime
    this
  }

  def expired(): SessionIntermediaryStateAssertions = {
    isActive = false
    this
  }

  def expectedPages(pages: Seq[VisitedPage]): SessionIntermediaryStateAssertions = {
    expectedPages = pages
    this
  }

  def validate(): Unit = {
    session.language shouldEqual language
    session.browser shouldEqual browser
    session.userId shouldEqual userId
    session.apiVersion shouldEqual apiVersion
    session.isActive shouldBe isActive
    session.expirationTimeMillisUtc shouldEqual expirationTime
    session.visitedPages.size shouldEqual expectedPages.size
    session.visitedPages should contain allElementsOf expectedPages
  }

}
