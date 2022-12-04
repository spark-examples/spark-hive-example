package org.example
import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveHive extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("SparkCreateTableExample")
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

}
