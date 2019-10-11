package com.waitingforcode.test

import org.apache.spark.sql.SparkSession

trait SparkSessionContext {

  val sparkSession = SparkSession.builder()
    .appName("Unit tests").master("local[*]").getOrCreate()

}
