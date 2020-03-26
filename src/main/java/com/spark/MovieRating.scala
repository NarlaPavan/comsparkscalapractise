package com.spark

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object MovieRating {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger("org.apache")
    logger.setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Movie Rating Analysis").master("local[1]").getOrCreate()
    val rating_df = spark.read.options(Map("header" -> "false")).csv("C:\\Users\\narla\\IdeaProjects\\comsparkscalapractise\\src\\main\\resources\\ratings.dat")

    val rating_df1 = rating_df.withColumn("userid", split(col("_c0"), "::").getItem(0).cast(IntegerType))
      .withColumn("movieid", split(col("_c0"), "::").getItem(1).cast(IntegerType))
      .withColumn("rating", split(col("_c0"), "::").getItem(2).cast(IntegerType))
      .withColumn("time", split(col("_c0"), "::").getItem(3).cast(LongType))
      .drop(col("_c0"))
    rating_df1.persist()
//    print(rating_df1.count())
//    rating_df1.show()
//    rating_df1.printSchema()

    //   spark.sqlContext.sql("show functions").show(250)
    spark.sqlContext.udf.register("convertEpoch",epochToTimestamp _)
    val convertEpoch = udf(epochToTimestamp _)

    val rating_df2 = rating_df1.withColumn("newtime",convertEpoch(col("time")))
    val rating_df3 = rating_df2.withColumn("newtime",col("newtime").cast(TimestampType)).drop("time")

    val rating_df4 = rating_df3.select(col("userid"),col("movieid"),col("rating"),year(col("newtime")) as "year",month(col("newtime")) as "month")
//    rating_df3.show()

//    rating_df4.write.saveAsTable("ratings.csv") // storing in hive
//    rating_df4.write.csv("C:\\Users\\narla\\Desktop\\output") // saving output in hdfs or local file system

    rating_df4.registerTempTable("ratings1")

    spark.sql("select * from ratings1 limit 10").show()


  }

  def epochToTimestamp(epochMills: Long): String = {
    val time: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    time.format(epochMills)
  }
}

