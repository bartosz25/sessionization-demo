package com.waitingforcode.streaming

import java.io.File

import com.waitingforcode.core.Json

object KafkaConfiguration {

  def createFromConfigurationFile(filePath: String): KafkaConfiguration =
    Json.Mapper.readValue(new File(filePath), classOf[KafkaConfiguration])


}

case class KafkaConfiguration(broker: String, inputTopic: String)