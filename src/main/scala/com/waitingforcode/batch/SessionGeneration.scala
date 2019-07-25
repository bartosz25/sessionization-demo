package com.waitingforcode.batch

import com.waitingforcode.core.{InputLogMapper, SessionIntermediaryState}
import org.apache.spark.sql.Row

object SessionGeneration {

  def resolveGroupByKey(log: Row): Long = {
    Option(InputLogMapper.userId(log))
      .getOrElse(SessionIntermediaryState.Mapper.userId(log))
  }

}
