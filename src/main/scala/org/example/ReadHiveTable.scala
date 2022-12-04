package org.example

import org.apache.spark.sql.SparkSession

object ReadHiveTable extends App {

  // Create SparkSession with hive enabled
  val spark = SparkSession.builder().master("local[*]")
    .appName("SparkByExamples.com")
    .enableHiveSupport()
    .getOrCreate()

  // Read table using table()
  val df = spark.read.table("emp.employee")
  df.show()

  // Read table using spark.sql()
  val df2 = spark.sql("select * from emp.employee")
  df2.show()
}
