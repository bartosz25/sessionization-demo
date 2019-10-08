package com.waitingforcode.batch

import com.waitingforcode.test.JoinedInputWithRestoredSession
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SessionGenerationCloseTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val TestSparkSession = SparkSession.builder()
      .master("local[*]").appName("Test session generation").getOrCreate()

  behavior of "data without input logs and with restored session"

  // TODO:
  // ^--- different creation methods for tested DataFrames:
  //      * https://stackoverflow.com/a/50183104/9726075
  //      * create 2 DataFrames and fullouter join them
  // to not block too long, I choose the method of custom case class
  // ^---- TODO: maybe there is a way to reconciliate it wit the defined schemas ?

  it should "close the session because of the window upper bound expiration time" in {
    import TestSparkSession.implicits._
    val inputRows = Seq(JoinedInputWithRestoredSession(userId = 100L, visitedPages = Seq.empty,
      isActive = true, browser = "Firefox", expirationTimeMillisUtc = 200L)).toDF().collect().toIterator

    val sessions = SessionGeneration.generate(100L, 900L)(100L, inputRows)

    sessions should have size 1
    sessions(0).isActive shouldBe false
  }

  it should "not close the session when TTL is not reached" in {
    import TestSparkSession.implicits._
    val inputRows = Seq(JoinedInputWithRestoredSession(userId = 100L, visitedPages = Seq.empty,
      isActive = true, browser = "Firefox", expirationTimeMillisUtc = 2200L)).toDF().collect().toIterator

    val sessions = SessionGeneration.generate(100L, 900L)(100L, inputRows)

    sessions should have size 1
    sessions(0).isActive shouldBe true
  }

  behavior of "input logs without restored session"

  it should "create 2 sessions because of too long inactivity time of the first one" in {

  }

}
