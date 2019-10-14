package com.waitingforcode.batch

import java.sql.Timestamp

import com.waitingforcode.test.Mocks
import org.apache.spark.sql.Row
import org.mockito.Mockito
import org.scalatest.{FlatSpec, Matchers}

class SessionGenerationTest extends FlatSpec with Matchers {

  behavior of "input resolved"

  it should "resolved restored session key" in {
    val mockedRow = Mockito.mock(classOf[Row])
    Mockito.when(mockedRow.getAs[Long]("userId")).thenReturn(100L)
    Mockito.when(mockedRow.getAs[String]("language")).thenReturn("abc")

    val groupByKey = SessionGeneration.resolveGroupByKey(mockedRow)

    groupByKey shouldEqual 100L
  }

  it should "resolve input log key" in {
    val mockedRow = Mockito.mock(classOf[Row])
    Mockito.when(mockedRow.getAs[Long]("user_id")).thenReturn(200L)
    Mockito.when(mockedRow.getAs[String]("language")).thenReturn(null)

    val groupByKey = SessionGeneration.resolveGroupByKey(mockedRow)

    groupByKey shouldEqual 200L
  }

  behavior of "logs dedupe and sort"

  it should "dedupe and sort input logs" in {
    val inputLogs = Seq(Mocks.inputLog("1970-01-01T00:00:40+00:00"),
      Mocks.inputLog("1970-01-01T00:00:10+00:00"), Mocks.inputLog("1970-01-01T00:00:20+00:00"),
      Mocks.inputLog("1970-01-01T00:00:10+00:00"), Mocks.inputLog("1970-01-01T00:00:30+00:00"))

    val sortedDedupedLogs = SessionGeneration.dedupedAndSortedLogs(inputLogs)

    sortedDedupedLogs should have size 4
    sortedDedupedLogs.map(row => row.getAs[Timestamp]("event_time")) should contain inOrderElementsOf(
      Seq(new Timestamp(10000L), new Timestamp(20000L), new Timestamp(30000L), new Timestamp(40000L))
    )
  }

}
