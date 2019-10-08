package com.waitingforcode.batch

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
    val inputLogs = Seq(inputLog("4"), inputLog("1"), inputLog("2"), inputLog("1"), inputLog("3"))

    val sortedDedupedLogs = SessionGeneration.dedupedAndSortedLogs(inputLogs)

    sortedDedupedLogs should have size 4
    sortedDedupedLogs.map(row => row.getAs[String]("event_time")) should contain inOrderElementsOf(
      Seq("1", "2", "3", "4")
    )
  }

  private def inputLog(eventTime: String): Row = {
    val mockedRow = Mockito.mock(classOf[Row])
    Mockito.when(mockedRow.getAs[String]("event_time")).thenReturn(eventTime)
    mockedRow
  }

}
