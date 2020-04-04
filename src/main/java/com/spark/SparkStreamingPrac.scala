package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._

object SparkStreamingPrac {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Streaming").master("local[*]").getOrCreate()
    spark.conf.set("spark.streaming.blockInterval", "200ms")
    val streamContext = new StreamingContext(spark.sparkContext, Seconds(10))

    val receiver = streamContext.socketTextStream("192.168.172.129", 12345)
    val receiver1 = streamContext.socketTextStream("192.168.172.129", 23456)
    val d = receiver.union(receiver1)

    //    val data1 = receiver.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey((x, y) => (x + y))
    //    data1.print()

    /*receiver.foreachRDD{
      x=>
        val p = x.getNumPartitions
        println(p)
    }*/

    d.foreachRDD {
      x =>
//        val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
        import spark.implicits._

        val ds = x.toDS()
        val df = x.toDF()
        val df1 = ds.flatMap(x=>x.split(" ")).map(x=>(x,1)).toDF().groupBy("_1").agg(count("*") as "count")
        df1.show()
    }
    streamContext.start()
    streamContext.awaitTermination()
    //    spark.close()
  }
}
