package com.waitingforcode.core

// TODO: add an ID
case class SessionOutput(userId: Long, site: String, apiVersion: String, browser: String,
                         language: String, eventTime: String, timeOnPageMillis: Long, page: String) {

}