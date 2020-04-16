package com.spark

import org.apache.spark.sql.SparkSession

object testUbuntuHdfsCluster {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TestUbuntuHDFSCluster").master("local").getOrCreate()
    val rd1= spark.sparkContext.textFile("hdfs://192.168.172.131:9000/sample")
    rd1.take(3).foreach(println)
    spark.close()
  }
}
