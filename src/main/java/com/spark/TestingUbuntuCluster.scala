package com.spark

import org.apache.spark.sql.SparkSession

object TestingUbuntuCluster {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Ubuntu Cluster").master("local[*]").getOrCreate()

    val rd1 = spark.sparkContext.textFile("hdfs://192.168.172.130:9000/ipaddress")
    rd1.take(10).foreach(println)
    spark.close()
  }
}
