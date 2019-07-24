package com.waitingforcode.core

import org.apache.spark.sql.Row

object InputLogMapper {

  // TODO: replace it with a constant or more intelligent object
  def visitId(log: Row): Long = log.getAs[Long]("visit_id")

  def userId(log: Row): Long = log.getAs[Long]("user_id")

  private def page(log: Row): Row = log.getAs[Row]("page")

  def currentPage(log: Row): String = page(log).getAs[String]("current")

  def eventTime(log: Row): Long = {
    log.getAs[String]("event_time") // TODO: need to parse it into a milliseconds
    0L
  }

}
