package com.waitingforcode.core

import com.waitingforcode.test.{Builders, SparkSessionContext}
import org.scalatest.{FlatSpec, Matchers}

class BatchWriterTest extends FlatSpec with Matchers with SparkSessionContext {
  import sparkSession.implicits._

  "batch writer" should "write only expired sessions" in {
    val sessions = Seq(
      Builders.sessionIntermediaryState(30L, Seq(VisitedPage(3000L, "a"), VisitedPage(4000L, "b"))),
      Builders.sessionIntermediaryState(31L, Seq(VisitedPage(3000L, "c"), VisitedPage(4000L, "d")), false),
      Builders.sessionIntermediaryState(32L, Seq(VisitedPage(5000L, "e"), VisitedPage(6000L, "f")), false)
    ).toDS()
    val outputDir = "/tmp/test-batch-writer"

    BatchWriter.writeDataset(sessions, outputDir)

    val writtenSessions = sparkSession.read.json(outputDir).as[SessionOutput]

    writtenSessions.count() shouldEqual 4
    writtenSessions.collect().map(output => output.page) should contain only ("c", "d", "e", "f")
  }

}
