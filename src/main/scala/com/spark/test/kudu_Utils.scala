package com.spark.test

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.kudu.spark._
import org.apache.kudu.client._
import collection.JavaConverters._
import org.apache.kudu.spark.kudu._
import collection.JavaConverters._

class kudu_Utils {
  def get_tables(tablename:String) ={
    val spark = SparkSession.builder
      .appName("TestKuduDF")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()
    val database:String = "DB10002."
    val table:String = database.concat(tablename)
    val kuduOptions = Map("kudu.master" -> "10.20.110.1:7051,10.20.110.2:7051,10.20.110.3:7051",
      "kudu.table" -> table,
      "kudu.operation.timeout.ms" -> "10000")
    val tableDF = spark.read.options(kuduOptions).kudu
    tableDF
  }
}
