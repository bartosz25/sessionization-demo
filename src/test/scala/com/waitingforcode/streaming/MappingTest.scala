package com.waitingforcode.streaming

import com.waitingforcode.core.{SessionIntermediaryState, VisitedPage}
import com.waitingforcode.test.Mocks
import org.mockito.{ArgumentCaptor, Mockito}
import org.scalatest.{FlatSpec, Matchers}

class MappingTest extends FlatSpec with Matchers {

  behavior of "streaming mapper"

  it should "map an expired session into expired session intermediary state" in {
    val stateBeforeUpdate = SessionIntermediaryState(
      userId = 2L, visitedPages = Seq.empty, browser = "Firefox", language = "fr",
      site = "site.html", apiVersion = "3.0", expirationTimeMillisUtc = Long.MaxValue,
      isActive = true
    )
    val state = Mocks.groupState(stateBeforeUpdate, true)

    val updatedState = Mapping.mapStreamingLogsToSessions(5000L)(stateBeforeUpdate.userId, Iterator.empty, state)

    updatedState.isActive shouldBe false
    Mockito.verify(state).remove()
  }

  it should "extend the state with new input logs" in {
    val stateBeforeUpdate = SessionIntermediaryState(
      userId = 2L, visitedPages = Seq(VisitedPage(1000L, "index.html"), VisitedPage(2000L, "index2.html")),
      browser = "Firefox", language = "fr", site = "site.html", apiVersion = "3.0",
      expirationTimeMillisUtc = 5000L, isActive = true
    )
    val state = Mocks.groupState(stateBeforeUpdate, false)
    Mockito.when(state.getCurrentWatermarkMs).thenReturn(3000L)
    val updatedStateCaptor = ArgumentCaptor.forClass(classOf[SessionIntermediaryState])
    val watermarkCaptor = ArgumentCaptor.forClass(classOf[Long])

    Mapping.mapStreamingLogsToSessions(5000L)(stateBeforeUpdate.userId,
      Iterator(
        Mocks.inputLog("1970-01-01T00:00:25+00:00", "a.html"), Mocks.inputLog("1970-01-01T00:00:35+00:00", "b.html")
      ), state)

    // since we're working on GroupState's mock, we should capture the updated event
    // we cannot use .get() because it will return the value from the mock
    Mockito.verify(state).update(updatedStateCaptor.capture())
    val newState = updatedStateCaptor.getValue
    newState.visitedPages should have size 4
    newState.visitedPages should contain allElementsOf (stateBeforeUpdate.visitedPages ++ Seq(
      VisitedPage(25000L, "a.html"), VisitedPage(35000L, "b.html")))
    newState.isActive shouldBe true
    Mockito.verify(state).setTimeoutTimestamp(watermarkCaptor.capture())
    watermarkCaptor.getValue shouldEqual 8000L
    Mockito.verify(state).update(org.mockito.Matchers.any(classOf[SessionIntermediaryState]))
  }

  it should "create a new state for new input logs" in {
    val state = Mocks.groupState(null, false)
    Mockito.when(state.getCurrentWatermarkMs).thenReturn(3000L)
    val updatedStateCaptor = ArgumentCaptor.forClass(classOf[SessionIntermediaryState])
    val watermarkCaptor = ArgumentCaptor.forClass(classOf[Long])

    Mapping.mapStreamingLogsToSessions(5000L)(3L,
      Iterator(
        Mocks.inputLog("1970-01-01T00:00:25+00:00", "a.html"), Mocks.inputLog("1970-01-01T00:00:35+00:00", "b.html")
      ), state)

    // since we're working on GroupState's mock, we should capture the updated event
    // we cannot use .get() because it will return the value from the mock
    Mockito.verify(state).update(updatedStateCaptor.capture())
    val newState = updatedStateCaptor.getValue
    newState.visitedPages should have size 2
    newState.visitedPages should contain allOf (VisitedPage(25000L, "a.html"), VisitedPage(35000L, "b.html"))
    newState.isActive shouldBe true
    Mockito.verify(state).setTimeoutTimestamp(watermarkCaptor.capture())
    watermarkCaptor.getValue shouldEqual 8000L
    Mockito.verify(state).update(org.mockito.Matchers.any(classOf[SessionIntermediaryState]))
  }

}
