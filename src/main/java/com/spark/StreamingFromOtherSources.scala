package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingFromOtherSources {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Streaming").master("local[*]").getOrCreate()
    spark.conf.set("spark.streaming.blockInterval", "500ms")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val ds1 = ssc.textFileStream("hdfs://192.168.172.129:8020/StreamingData")
    ds1.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
