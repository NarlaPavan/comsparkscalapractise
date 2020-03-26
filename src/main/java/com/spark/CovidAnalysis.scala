package com.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object CovidAnalysis {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("org.apache")
    logger.setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Covid Analysis").master("local[1]").getOrCreate()

    val covid_schema = StructType(
      StructField("Province/State", StringType)
        :: StructField("Province/State", StringType)
        :: StructField("Country/Region", StringType)
        :: StructField("Lat", DoubleType)
        :: StructField("Long", DoubleType)
        :: StructField("Date", TimestampType)
        :: StructField("Confirmed", IntegerType)
        :: StructField("Deaths", IntegerType)
        :: Nil
    )

    val covid_df = spark.read.options(Map("header" -> "true", "inferschema" -> "true", "delimeter" -> ",")).csv("C:\\Users\\narla\\IdeaProjects\\comsparkscalapractise\\src\\main\\resources\\complete_data_new_format.csv")
    covid_df.show(20)
    val covid_df1 = covid_df.withColumn("Date",col("Date").cast(TimestampType))

//    covid_df1.groupBy(month(col("Date"))).agg(sum(col("Confirmed")) as "total confirmed",sum(col("Deaths")) as "Total Deaths").show()
    covid_df1.printSchema()
    covid_df1.show(20)
  }
}
