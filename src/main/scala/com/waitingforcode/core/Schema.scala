package com.waitingforcode.core

import org.apache.spark.sql.types._

object Visit {

  val Schema = new StructBuilder()
    .withRequiredFields(
      Map(
        "user_id" -> fields.long, "event_time" -> fields.timestamp,
        "page" -> fields.newStruct.withRequiredFields(Map("current" -> fields.string))
          .withOptionalFields(Map("previous" -> fields.string)).toField,
        "source" -> fields.newStruct.withRequiredFields(Map(
          "site" -> fields.string, "api_version" -> fields.string)
        ).toField,
        "user" -> fields.newStruct.withRequiredFields(Map(
          "ip" -> fields.string, "latitude" -> fields.double, "longitude" -> fields.double
        )).toField,
        "technical" -> fields.newStruct.withRequiredFields(Map(
          "browser" -> fields.string, "os" -> fields.string, "lang" -> fields.string, "network" -> fields.string,
          "device" -> fields.newStruct.withRequiredFields(
            Map("type" -> fields.string, "version" -> fields.string)
          ).toField
        )).toField
      )
    ).buildSchema

}


class StructBuilder() {

  private var structFields = Array.empty[StructField]

  def withRequiredFields = withFields(false) _

  def withOptionalFields = withFields(true) _

  private def withFields(nullable: Boolean)(creators: Map[String, (String, Boolean) => StructField]) = {
    val mappedFields = creators.map {
      case (fieldName, fieldGenerator) => fieldGenerator(fieldName, nullable)
    }
    structFields = mappedFields.toArray ++ structFields
    this
  }

  def build: Array[StructField] = structFields

  def buildSchema: StructType = StructType(structFields)

  def toField = fields.struct(structFields)
}

object fields {
  private def baseField(fieldType: DataType)(fieldName: String, nullable: Boolean) =
    new StructField(fieldName, fieldType, nullable)
  def string = baseField(StringType) _
  def timestamp = baseField(TimestampType) _
  def array(fieldType: DataType, nullableContent: Boolean) = baseField(ArrayType(fieldType, nullableContent)) _
  def struct(fields: Array[StructField]) = baseField(StructType(fields)) _
  def long = baseField(LongType) _
  def integer = baseField(IntegerType) _
  def double = baseField(DoubleType) _
  def boolean = baseField(BooleanType) _

  def newStruct = new StructBuilder()

}