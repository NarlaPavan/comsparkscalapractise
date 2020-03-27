package com.spark

import org.apache.spark.sql.SparkSession

object Demo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RDD GRAPHS").master("local").getOrCreate()

    val df1 = spark.read.csv("./src/main/resources/csvfile1")
    df1.show(10)

  }
}
