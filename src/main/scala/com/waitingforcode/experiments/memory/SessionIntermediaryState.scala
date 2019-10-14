package com.waitingforcode.experiments.memory

import com.waitingforcode.core.{SessionIntermediaryState, StructBuilder, VisitedPage, fields}

object SessionIntermediaryState2 {

    def createNew(logs: Iterator[JoinedLog], timeoutDurationMs: Long): SessionIntermediaryState = {
      val materializedLogs = logs.toSeq
      val visitedPages = mapInputLogsToVisitedPages(materializedLogs)
      val headLog = materializedLogs.head

      SessionIntermediaryState(userId = headLog.user_id.get, visitedPages = visitedPages, isActive = true,
        browser = headLog.technical.map(t => t.browser).get,
        language = headLog.technical.map(t => t.lang).get,
        site = headLog.source.map(s => s.site).get,
        apiVersion = headLog.source.map(s => s.api_version).get,
        expirationTimeMillisUtc = getTimeout(visitedPages.last.eventTime, timeoutDurationMs)
      )
    }

    def restoreFromRow(row: JoinedLog, isActive: Boolean = true) = {
      SessionIntermediaryState(
        userId = row.userId.get, browser = row.browser.get, language = row.language.get,
        site = row.site.get, apiVersion = row.apiVersion.get,
        expirationTimeMillisUtc = row.expirationTimeMillisUtc.get,
        isActive = isActive, visitedPages = row.visitedPages.get
      )
    }

    def getTimeout(eventTime: Long, timeoutDurationMs: Long) = eventTime + timeoutDurationMs

    private def mapInputLogsToVisitedPages(logs: Seq[JoinedLog]): Seq[VisitedPage] =
      logs.filter(joinedLog => joinedLog.event_time.isDefined)
          .map(log => VisitedPage(log.eventTimeMillis.get, log.page.get.current))
          .sortBy(log => log.eventTime)

    val Schema = new StructBuilder()
      .withRequiredFields(Map(
        "userId" -> fields.long,
        "visitedPages" -> fields.array(
          fields.newStruct.withRequiredFields(
            Map("eventTime" -> fields.long, "pageName" -> fields.string)
          ).buildSchema, nullableContent = false
        ),
        "isActive" -> fields.boolean
      )).buildSchema
}
