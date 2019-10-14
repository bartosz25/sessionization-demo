package com.waitingforcode.batch

import java.io.File

import com.waitingforcode.core.{SessionIntermediaryState, VisitedPage}
import com.waitingforcode.test.{Builders, SparkSessionContext}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class DataLoaderTest extends FlatSpec with Matchers with SparkSessionContext with BeforeAndAfterAll {

  import sparkSession.implicits._

  private val previousSessionsDir = "/tmp/previous-sessions"

  override def beforeAll() {
    val sessions = Seq(
      Builders.sessionIntermediaryState(30L, Seq(VisitedPage(3000L, "a"), VisitedPage(4000L, "b"))),
      Builders.sessionIntermediaryState(31L, Seq(VisitedPage(3000L, "c"), VisitedPage(4000L, "d"))),
      Builders.sessionIntermediaryState(32L, Seq(VisitedPage(5000L, "e"), VisitedPage(6000L, "f")), false)
    ).toDS()
    sessions.write.json(previousSessionsDir)
  }

  override def afterAll() {
    FileUtils.deleteDirectory(new File(previousSessionsDir))
  }

  behavior of "loading previous window sessions"

  it should "load an empty DataFrame if the directory is empty" in {
    val loadedSessions = DataLoader.loadPreviousWindowSessions(sparkSession, None)

    loadedSessions.count() shouldBe 0
  }

  it should "load only active sessions if the directory exists" in {
    val loadedSessions = DataLoader.loadPreviousWindowSessions(sparkSession, Some(previousSessionsDir))
        .as[SessionIntermediaryState].collect()

    loadedSessions should have size 2
    loadedSessions.map(session => session.userId) should contain only (30L, 31L)
  }



}
