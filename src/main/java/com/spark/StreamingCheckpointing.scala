package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingCheckpointing {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Checkpointing").master("local[*]").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(20))
    ssc.checkpoint("hdfs://192.168.172.129:8020/checkpoint1")
    val ds1 = ssc.socketTextStream("192.168.172.129", 12345)
    val ds2 = ds1.map(x => x + " stream")
    val ds3 = ds2.map(x => x + " streaming")
    ds3.checkpoint(Seconds(20))
    ds3.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
