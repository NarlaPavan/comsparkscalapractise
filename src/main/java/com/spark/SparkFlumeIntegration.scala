package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkFlumeIntegration {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spar Flume Integration").master("local[*]").getOrCreate()

    val batchInterval = args(0).toInt
    val host = args(1)
    val portno = args(2).toInt

    val ssc = new StreamingContext(spark.sparkContext, Seconds(batchInterval))

    val flumeData = FlumeUtils.createStream(ssc, host, portno)

    val res = flumeData.map {
      sparkFlumeEvent =>
        val event = sparkFlumeEvent.event
        val messageBody = new String(event.getBody.array())
        messageBody
    }

    res.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
