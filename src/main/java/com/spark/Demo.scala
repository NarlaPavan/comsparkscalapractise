package com.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Demo {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("org.apache")
    logger.setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("UseCase").master("local").getOrCreate()
    val df1 = spark.read.csv("D:\\Spark_Java\\Computer_Business\\WithoutHeaders\\Customer.csv")

    //    df1.show(10)

    //FNAME	LNAME	STATUS	TELNO	CUSTOMER_ID	CITY|ZIP

    val df2 = df1.withColumn("city", split(col("_c5"), "\\|").getItem(0))
      .withColumn("zip", split(col("_c5"), "\\|").getItem(1))
      .drop("_c5")
      .withColumnRenamed("_c0", "fname")
      .withColumnRenamed("_c1", "lname")
      .withColumnRenamed("_c2", "status")
      .withColumnRenamed("_c3", "telnum")
      .withColumnRenamed("_c4", "fcustid")

//    df2.show(10)
    df2.printSchema()
    print(df2.count)
    spark.close()
  }
}
