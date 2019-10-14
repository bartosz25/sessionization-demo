package com.waitingforcode.experiments.memory

import java.time.ZonedDateTime

import com.waitingforcode.core.VisitedPage

case class InputLog(user_id: Long, event_time: String, page: Page, source: Source, user: User,
                    technical: Technical)

case class Page(current: String, previous: Option[String])
case class Source(site: String, api_version: String)
case class User(ip: String, latitude: Double, longitude: Double)
case class Technical(browser: String, os: String, lang: String, network: String, device: Device)
case class Device(`type`: String, version: String)

// TODO: add later session data
case class JoinedLog(user_id: Option[Long], event_time: Option[String], page: Option[Page],
                     source: Option[Source], user: Option[User],
                     technical: Option[Technical], userId: Option[Long], visitedPages: Option[Seq[VisitedPage]],
                     browser: Option[String], language: Option[String], site: Option[String],
                     apiVersion: Option[String], expirationTimeMillisUtc: Option[Long], isActive: Option[Boolean]) {

  def groupByKey = user_id.getOrElse(userId.get)

  def eventTimeMillis = {
    event_time.map(eventTime => {
      ZonedDateTime.parse(eventTime).toInstant.toEpochMilli
    })
  }

}