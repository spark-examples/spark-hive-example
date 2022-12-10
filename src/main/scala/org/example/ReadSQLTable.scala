package org.example

import org.apache.spark.sql.SparkSession

object ReadSQLTable {

  // Create SparkSession
  val spark = SparkSession.builder().master("local[*]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  import spark.implicits._

  // Create DataFrame
  val sampleDF = Seq((1, "James",30,"M"),
    (2, "Ann",40,"F"), (3, "Jeff",41,"M"),
    (4, "Jennifer",20,"F")
  ).toDF("id", "name","age","gender")

  // Write to SQL Table
  sampleDF.write
  .format("com.microsoft.sqlserver.jdbc.spark")
  .mode("overwrite")
  .option("url", "jdbc:sqlserver://{SERVER_ADDR};databaseName=emp;")
  .option("dbtable", "employee")
  .option("user", "replace_user_name")
  .option("password", "replace_password")
  .save()

  // Read from SQL Table
  val df = spark.read
  .format("com.microsoft.sqlserver.jdbc.spark")
  .option("url", "jdbc:sqlserver://{SERVER_ADDR};databaseName=emp;")
  .option("dbtable", "employee")
  .option("user", "replace_user_name")
  .option("password", "replace_password")
  .load()

  // Show sample records from data frame
  df.show(5)
}
