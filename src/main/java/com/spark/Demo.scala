package com.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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
    df3.withColumn("citygroup", when(col("custid") >= 1 and col("custid") <= 1000, "oldcustomer").when(col("custid") >= 1001 and col("custid") <= 2000, "midcustomer").otherwise("newcustomers")).show()

    //modify column name
    df3.withColumnRenamed("city", "cities").show

    df3.withColumn("newzip", col("zip") + 10).show

    df3.withColumn("age", lit(10)).show

    val prod = spark.read.option("header", "true").csv("C:\\Users\\narla\\IdeaProjects\\comsparkscalapractise\\src\\main\\resources\\Product.csv")

    prod.show()

    val df_csv1 = spark.read.option("inferSchema", "true").csv("C:\\Users\\narla\\IdeaProjects\\comsparkscalapractise\\src\\main\\resources\\csvfile1")

    df_csv1.show

    df_csv1.printSchema()

    val sch = StructType(
      StructField("id1",FloatType)
        ::StructField("id2",FloatType)
        ::StructField("id3",FloatType)
        ::Nil)

    val df_csv2 = spark.read.option("inferSchema", "true").schema(sch).csv("C:\\Users\\narla\\IdeaProjects\\comsparkscalapractise\\src\\main\\resources\\csvfile1")

    df_csv2.printSchema()

    val ele = spark.read.option("inferSchema", "true").option("delimiter","\t").option("header",true).csv("C:\\Users\\narla\\IdeaProjects\\comsparkscalapractise\\src\\main\\resources\\elections2014.tsv")

    ele.show(15)

    //grouping operations
// state|constituency|      candidate_name| sex| age|category|partyname|   partysymbol|general|postal| total|pct_of_total_votes|pct_of_polled_votes|totalvoters|

    ele.groupBy(col("state"),col("partyname")).agg(sum("total") as "total_votes").sort("state","partyname").show

    ele.select("state","partyname").where("state == 'Andhra Pradesh'").groupBy("state","partyname").agg(count("*") as "count").sort("count","partyname").show(100)

    val ap_df = ele.select("*").where("state =='Andhra Pradesh'")
    ap_df.groupBy("state","constituency").agg(max("total")).sort("constituency").show(100)


    spark.close()
  }
}
