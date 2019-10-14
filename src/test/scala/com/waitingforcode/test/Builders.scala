package com.waitingforcode.test

import com.waitingforcode.core.{SessionIntermediaryState, VisitedPage}

object Builders {

  def sessionIntermediaryState(userId: Long, visitedPages: Seq[VisitedPage],
                               isActive:Boolean = true): SessionIntermediaryState = {
    SessionIntermediaryState(
      userId = userId, visitedPages = visitedPages, browser = "Firefox", language = "en",
      site = "http:", apiVersion = "1.0", expirationTimeMillisUtc = 1000L, isActive = isActive
    )
  }

}
