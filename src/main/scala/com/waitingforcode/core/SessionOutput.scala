package com.waitingforcode.core

case class SessionOutput(userId: Long, visitId: Long, source: String, apiVersion: String, browser: String,
                         language: String, eventTime: String, timeOnPageMillis: Long, page: String) {

  // TODO: maybe use a timestamp? millis?
  def year = eventTime.take(4)
  def month = eventTime.slice(6, 8)
  def day = eventTime.slice(8, 10)
  def hour = eventTime.slice(10, 12)

}