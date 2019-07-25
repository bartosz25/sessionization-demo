package com.waitingforcode.core

import com.waitingforcode.core.InputLogMapper.{currentPage, eventTime}
import org.apache.spark.sql.Row

case class SessionIntermediaryState(userId: Long, visitedPages: Iterator[VisitedPage], isActive: Boolean) {

  def updateWithNewLogs(newLogs: Iterator[Row]): SessionIntermediaryState = {
    val newVisitedPages = SessionIntermediaryState.mapInputLogsToVisitedPages(newLogs)
    this.copy(visitedPages = visitedPages ++ newVisitedPages)
  }

  // TODO: implement me
  def toSessionOutputState: Seq[SessionOutput] = Seq.empty

}

case class VisitedPage(eventTime: Long, pageName: String)
object VisitedPage {

  def fromInputLog(log: Row): VisitedPage = {
    VisitedPage(eventTime(log), currentPage(log))
  }

}

object SessionIntermediaryState {

  object Mapper {
    def userId(session: Row) = session.getAs[Long]("userId")
  }

  def createNew(logs: Iterator[Row]): SessionIntermediaryState = {
    val headLog = logs.next()
    val allLogs = Iterator(VisitedPage.fromInputLog(headLog)) ++
      SessionIntermediaryState.mapInputLogsToVisitedPages(logs)

    SessionIntermediaryState(userId = InputLogMapper.userId(headLog), visitedPages = allLogs, isActive = true)
  }

  private[core] def mapInputLogsToVisitedPages(logs: Iterator[Row]): Iterator[VisitedPage] =
    logs.map(log => VisitedPage.fromInputLog(log))

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
