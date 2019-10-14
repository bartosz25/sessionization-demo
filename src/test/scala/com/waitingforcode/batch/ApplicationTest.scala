package com.waitingforcode.batch

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SparkSession, functions}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ApplicationTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val baseDir = "/tmp/sessionization-demo-test/batch"
  private val inputBaseDir = s"${baseDir}/input"
  private val inputLogsProcessingWindow1 = s"${inputBaseDir}/2019/01/28/10"
  private val inputLogsProcessingWindow2 = s"${inputBaseDir}/2019/01/28/11"
  private val outputBaseDir = s"${baseDir}/output"

  override def beforeAll(): Unit = {
    FileUtils.forceMkdir(new File(inputLogsProcessingWindow1))
    FileUtils.forceMkdir(new File(inputLogsProcessingWindow2))
    val dataWindow1 =
      """
        |{"user": {"latitude": -27.0782, "longitude": -65.9743, "ip": "1.1.1.1"}, "source": {"site": "mysite.com", "api_version": "v3"}, "user_id": 1, "page": {"previous": "article category 2-10", "current": "category 18"}, "visit_id": "2f8340bc3f3247298162305bd1979ff5", "event_time": "2019-01-28T10:26:45+00:00", "technical": {"network": "adsl", "lang": "en", "device": {"type": "tablet", "version": "Samsung Galaxy Tab S3"}, "os": "Android 8.1", "browser": "Mozilla Firefox 53"}}
        |{"user": {"latitude": -27.0782, "longitude": -65.9743, "ip": "1.1.1.1"}, "source": {"site": "mysite.com", "api_version": "v3"}, "user_id": 1, "page": {"previous": "category 18", "current": "category 20"}, "visit_id": "2f8340bc3f3247298162305bd1979ff5", "event_time": "2019-01-28T10:59:25+00:00", "technical": {"network": "adsl", "lang": "en", "device": {"type": "tablet", "version": "Samsung Galaxy Tab S3"}, "os": "Android 8.1", "browser": "Mozilla Firefox 53"}}
      """.stripMargin
    FileUtils.writeStringToFile(new File(s"${inputLogsProcessingWindow1}/input.json"), dataWindow1)
    val dataWindow2 =
      """
        |{"user": {"latitude": -27.0782, "longitude": -65.9743, "ip": "1.1.1.1"}, "source": {"site": "mysite.com", "api_version": "v3"}, "user_id": 1, "page": {"previous": "category 20", "current": "category 22"}, "visit_id": "2f8340bc3f3247298162305bd1979ff5", "event_time": "2019-01-28T11:02:45+00:00", "technical": {"network": "adsl", "lang": "en", "device": {"type": "tablet", "version": "Samsung Galaxy Tab S3"}, "os": "Android 8.1", "browser": "Mozilla Firefox 53"}}
      """.stripMargin
    FileUtils.writeStringToFile(new File(s"${inputLogsProcessingWindow2}/input.json"), dataWindow2)
  }

  override def afterAll(): Unit = {
    FileUtils.forceDelete(new File(baseDir))
  }

  "sessions" should "be generated for 2 processing windows" in {
    Application.main(Array(s"${inputLogsProcessingWindow1}/*", "", "2019-01-28 10", outputBaseDir))

    val sparkSession = SparkSession.builder()
      .appName("Assert on generated values").master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val openSessions20190128 = sparkSession.read.json(s"${outputBaseDir}/open-sessions/2019/01/28/10")
      .select(functions.concat_ws(";", $"userId", $"expirationTimeMillisUtc").as("tested_value")).collect().map(row => row.getAs[String]("tested_value"))
    openSessions20190128 should have size 1
    openSessions20190128 should contain only("1;1548673465000")
    val generatedSessions20190128 = sparkSession.read.json(s"${outputBaseDir}/sessions-output/2019/01/28/10")
      .select(functions.concat_ws(";", $"id", $"page").as("tested_value")).collect().map(row => row.getAs[String]("tested_value"))
    generatedSessions20190128 should have size 1
    generatedSessions20190128 should contain only("36517317;category 18")


    Application.main(Array(s"${inputLogsProcessingWindow2}/*", s"${outputBaseDir}/open-sessions/2019/01/28/10", "2019-01-28 11", outputBaseDir))
    val generatedSessions2019012811 = sparkSession.read.json(s"${outputBaseDir}/sessions-output/2019/01/28/11")
      .select(functions.concat_ws(";", $"id", $"page").as("tested_value")).collect().map(row => row.getAs[String]("tested_value"))
    generatedSessions2019012811 should have size 2
    generatedSessions2019012811 should contain allOf("-706852745;category 20", "-706852745;category 22")
    val openSessions2019012811 = sparkSession.read.json(s"${outputBaseDir}/open-sessions/2019/01/28/11").count()
    openSessions2019012811 shouldEqual 0L
  }

}
