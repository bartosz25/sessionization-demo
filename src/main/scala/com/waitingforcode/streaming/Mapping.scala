package com.waitingforcode.streaming

import com.waitingforcode.core.SessionIntermediaryState
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.GroupState

object Mapping {

  def mapStreamingLogsToSessions(timeoutDurationMs: Long)(key: Long, logs: Iterator[Row],
                                 currentState: GroupState[SessionIntermediaryState]): SessionIntermediaryState = {
    // TODO: figure out what is the correlation between state timeout and event-time watermark ?
    //       For instance, if the state expires, does the watermark is updated ? If watermark is reached, does the
    //       state expires? And so on.
    //       Note for myself 28.07.2019: now I ignore this problematic.
    if (currentState.hasTimedOut) {
      currentState.get.expire
    } else {
      val newState = currentState.getOption.map(state => state.updateWithNewLogs(logs, timeoutDurationMs))
        .getOrElse(SessionIntermediaryState.createNew(logs, timeoutDurationMs))
      currentState.update(newState)
      // TODO: talk about different possibilities to configure the timeout; here we're using event-time based but
      //       you can also use processing time-based;
      // TODO: talk also about what it involves
      currentState.setTimeoutTimestamp(newState.expirationTimeMillisUtc)
      // So here I'm using event-time expiration but in order to enable it, you must set the watermark on the query
      // Exception in thread "main" org.apache.spark.sql.AnalysisException: Watermark must be specified in the query using '[Dataset/DataFrame].withWatermark()' for using event-time timeout in a [map|flatMap]GroupsWithState. Event-time timeout not supported without watermark.;;
      // TODO: SerializeFromObject [mapobjects(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), if (isnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true))) null else named_struct(userId, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).userId, site, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).site, true, false), apiVersion, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).apiVersion, true, false), browser, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).browser, true, false), language, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).language, true, false), eventTime, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).eventTime, true, false), timeOnPageMillis, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).timeOnPageMillis, page, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(lambdavariable(MapObjects_loopValue1, MapObjects_loopIsNull1, ObjectType(class com.waitingforcode.core.SessionOutput), true)).page, true, false)), unwrapoption(ObjectType(interface scala.collection.Seq), input[0, scala.Option, true]), None) AS value#37]
      //+- FlatMapGroupsWithState org.apache.spark.sql.KeyValueGroupedDataset$$Lambda$1050/9126317@600bbf9e, cast(value#26 as string).toString, createexternalrow(key#21.toString, value#22.toString, StructField(key,StringType,true), StructField(value,StringType,true)), [value#26], [key#21, value#22], obj#36: scala.Option, class[userId[0]: bigint, visitedPages[0]: array<struct<eventTime:bigint,pageName:string>>, browser[0]: string, language[0]: string, site[0]: string, apiVersion[0]: string, expirationTimeMillisUtc[0]: bigint, isActive[0]: boolean], Update, true, EventTimeTimeout
      //   +- AppendColumns com.waitingforcode.streaming.Application$$$Lambda$984/1651379334@18d30e7, interface org.apache.spark.sql.Row, [StructField(key,StringType,true), StructField(value,StringType,true)], createexternalrow(key#21.toString, value#22.toString, StructField(key,StringType,true), StructField(value,StringType,true)), [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false) AS value#26]
      // TODO: I suppose here that it also deserves its own explanation in the post
      // Note for myself: In fact either I specify an event-time watermark or a processing time watermark, I've always
      //                  as risk to miss the data. For the event-time watermark, my example with users from Asia is a
      //                  good proof of that. Just to recall, these users can encounter a network latency much more
      //                  important than the session timeout. And in such a case I'd create incorrect sessions.
      //                  For event-time processing, the story is the same. If a user has a smaller latency,
      //                  his data will be also delivered at late.
      //                  ^--- *BUT* it applies to batch processing as well, unless we compute sessions with much
      //                             bigger delays, like 4-6 hours. On the other side, if you keep a watermark for
      //                             6 hours, you will likely require a lot of hardware to keep it in place.
      //                  So that's after this error that I'm adding an event-time watermark to my streaming query.
      // TODO: I already see a problem with the relationship between state timeout and watermark:
      //       Caused by: java.lang.IllegalArgumentException: Timeout timestamp (1564291203000) cannot be earlier than the current watermark (1564291872001)
      //	at org.apache.spark.sql.execution.streaming.GroupStateImpl.setTimeoutTimestamp(GroupStateImpl.scala:114)
      //	at com.waitingforcode.streaming.Mapping$.mapStreamingLogsToSessions(Mapping.scala:24)
      //	at com.waitingforcode.streaming.Application$.$anonfun$main$2(Application.scala:58)
      //	at com.waitingforcode.streaming.Application$.$anonfun$main$2$adapted(Application.scala:58)
      //	at org.apache.spark.sql.KeyValueGroupedDataset.$anonfun$mapGroupsWithState$2(KeyValueGroupedDataset.scala:279)
      //       It was for: .withWatermark("timestamp", "3 minutes") AND currentState.setTimeoutTimestamp(newState.expirationTimeMillisUtc)
      //  TODO: figure out what does this relationship involve?
      currentState.get
    }
  }

}
