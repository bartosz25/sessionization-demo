package com.waitingforcode.core

case class SessionOutput(id: Long, userId: Long, site: String, apiVersion: String, browser: String,
                         language: String, eventTime: String, timeOnPageMillis: Long, page: String) {

}