package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingPrac {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Streaming").master("local").getOrCreate()
    val streamContext = new StreamingContext(spark.sparkContext, Seconds(20))

    val receiver = streamContext.socketTextStream("localhost",12345)
     val data1 = receiver.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((x,y)=>(x+y))
    data1.print()
    streamContext.start()
    streamContext.stop(false,true)
    spark.close()
  }
}
