package org.example
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File

object SaveHive extends App {
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  val spark = SparkSession.builder().master("local[*]")
    .appName("SparkByExamples.com")
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

  import spark.implicits._

  // Create DataFrame
  val df2 = Seq(
    (1, "Tiger"),
    (2, "Lion"),
    (3, "Monkey")
  ).toDF("id", "animal")

  // Create temporary view
  df2.createOrReplaceTempView("sampleView")

  //Create a Database CT
  spark.sql("CREATE DATABASE IF NOT EXISTS ct")

  //Create a Table naming as sampleTable under CT database.
  spark.sql("CREATE TABLE ct.sampleTable (number Int, word String)")

  //Insert into sampleTable using the sampleView.
  spark.sql("INSERT INTO TABLE ct.sampleTable  SELECT * FROM sampleView")

  //Lets view the data in the table
  spark.sql("SELECT * FROM ct.sampleTable").show()
}
