package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingWindowing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Streaming Windowing").master("spark://quickstart.cloudera:6066").getOrCreate()
    spark.conf.set("spark.streaming.blockInterval", "500ms")
    spark.conf.set("spark.executor.memory", "1G")
    spark.conf.set("spark.executor.cores", "1")

    spark.conf.set("spark.driver.cores", "1")
    spark.conf.set("spark.driver.memory", "2G")

    spark.conf.set("spark.deploy.mode", "cluster")
    val streamContext = new StreamingContext(spark.sparkContext, Seconds(10))

    val receiver1 = streamContext.socketTextStream("192.168.172.129", 12345)

    val ds1 = receiver1.window(Seconds(20))

    ds1.print()
    streamContext.start()
    streamContext.awaitTermination()
  }
}
