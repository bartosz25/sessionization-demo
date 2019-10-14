package com.waitingforcode.core

import java.sql.Timestamp

import org.apache.spark.sql.Row

object InputLogMapper {

  def userId(log: Row): Long = log.getAs[Long]("user_id")

  private def page(log: Row): Row = log.getAs[Row]("page")

  def currentPage(log: Row): String = page(log).getAs[String]("current")

  def eventTime(log: Row): Long = {
    val eventTime = eventTimeTimestamp(log)
    eventTime.toInstant.toEpochMilli
  }

  def eventTimeTimestamp(log: Row): Timestamp = log.getAs[Timestamp]("event_time")

  private def technical(log: Row): Row = log.getAs[Row]("technical")

  def browser(log: Row): String = technical(log).getAs[String]("browser")

  def language(log: Row): String = technical(log).getAs[String]("lang")

  private def source(log: Row) = log.getAs[Row]("source")

  def site(log: Row): String = source(log).getAs[String]("site")

  def apiVersion(log: Row): String = source(log).getAs[String]("api_version")

}
