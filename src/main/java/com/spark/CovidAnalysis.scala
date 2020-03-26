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
//    covid_df.show(20)
    val covid_df1 = covid_df.withColumn("Date",to_timestamp(col("Date"),"MM/dd/yy"))

    val covid_grp = covid_df1.groupBy(dayofmonth(col("Date")) as "day",month(col("Date")) as "month").agg(sum(col("Confirmed")) as "Total_Confirmed",sum(col("Deaths")) as "Total_Deaths").sort(col("month"),col("day"),col("Total_Confirmed"))
    covid_grp.write.csv("C:\\Users\\narla\\Desktop\\covid_data")
//    covid_df1.printSchema()
    covid_df1.show(20)
  }
}
