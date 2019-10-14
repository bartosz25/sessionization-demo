package com.waitingforcode.core

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object Json {

  val Mapper = new ObjectMapper() with ScalaObjectMapper
  Mapper.registerModule(DefaultScalaModule)

}
