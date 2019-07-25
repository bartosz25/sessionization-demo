package com.waitingforcode.batch

import com.waitingforcode.core.SessionIntermediaryState
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

// TODO: solve the problem of SparkSession !
class DataLoaderTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    val Xsessions = Seq(
      activeSession(1L), inactiveSession(2L), activeSession(3L)
    )
  }

  private val TestSparkSession = SparkSession.builder()
      .appName("DataLoaderTest").getOrCreate()

  behavior of "loading previous window sessions"

  it should "load an empty DataFrame if the directory is empty" in {
    val loadedSessions = DataLoader.loadPreviousWindowSessions(TestSparkSession, None)

    loadedSessions.count() shouldBe 0
  }

  it should "load only active sessions if the directory exists" in {
    // TODO: add a test here
  }


  def activeSession = intermediarySession(true)
  def inactiveSession = intermediarySession(false)

  def intermediarySession(active: Boolean)(userId: Long): SessionIntermediaryState = {

  }

}
