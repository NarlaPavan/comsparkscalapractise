package com.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}

object Demo {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("org.apache")
    logger.setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("UseCase").master("local").getOrCreate()
    // loading customer data
    val df1 = spark.read.csv("D:\\Spark_Java\\Computer_Business\\WithoutHeaders\\Customer.csv")

    // splitting _cs colmn to two columns and modifying column names
    val df2 = df1.withColumn("city", split(col("_c5"), "\\|").getItem(0))
      .withColumn("zip", split(col("_c5"), "\\|").getItem(1))
      .drop("_c5")
      .withColumnRenamed("_c0", "fname")
      .withColumnRenamed("_c1", "lname")
      .withColumnRenamed("_c2", "status")
      .withColumnRenamed("_c3", "telnum")
      .withColumnRenamed("_c4", "custid")

    df2.printSchema()
    val df3 = df2.withColumn("fname", col("fname").cast(StringType))
      .withColumn("lname", col("lname").cast(StringType))
      .withColumn("status", col("status").cast(StringType))
      .withColumn("telnum", col("telnum").cast(StringType))
      .withColumn("custid", col("custid").cast(IntegerType))
      .withColumn("city", col("city").cast(StringType))
      .withColumn("zip", col("zip").cast(IntegerType))


    df3.printSchema()


    //selecting distinct status
    df3.select("status").distinct().show()

    //filter
    df3.where("city == 'Bigcity'").show()

    //distinct
    df3.select("city").distinct().show()

    //modify data of the column
    df3.withColumn("city", when(col("city") === "Smallcity", 0).otherwise(10)).show()

    df3.show(10)

    // new column
    df3.withColumn("citygroup",when(col("custid")>= 1 and col("custid") <= 1000,"oldcustomer").when(col("custid") >= 1001 and col("custid") <= 2000,"midcustomer").otherwise("newcustomers")).show()

    //modify column name
    df3.withColumnRenamed("city","cities").show

    
    spark.close()
  }
}
