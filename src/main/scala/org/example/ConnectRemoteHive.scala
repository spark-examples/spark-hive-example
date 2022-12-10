package org.example

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

object ConnectRemoteHive extends App{

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  val spark = SparkSession.builder().master("local[*]")
    .appName("SparkByExamples.com")
    .config("hive.metastore.uris", "thrift://192.168.1.190:9083")
    .config("spark.sql.warehouse.dir","/users/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  // Create DataFrame
  val sampleDF = Seq((1, "James",30,"M"),
    (2, "Ann",40,"F"), (3, "Jeff",41,"M"),
    (4, "Jennifer",20,"F")
  ).toDF("id", "name","age","gender")

  spark.sql("CREATE DATABASE IF NOT EXISTS emp")
  // Create Hive Internal table
  sampleDF.write.mode(SaveMode.Overwrite)
    .saveAsTable("emp.employee")



  spark.read.table("emp.employee").show()
}
