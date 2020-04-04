package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkTwitter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Twitter analysis").master("local[*]").getOrCreate()

//    spark.sparkContext.setLogLevel("ERROR")

    System.setProperty("twitter4j.oauth.consumerKey", "kn4frtAhecw1IHivBi4ExMDy1")
    System.setProperty("twitter4j.oauth.consumerSecret", "qPOEnlm70WdGRaevCJ4GDyd65OQqyIHpO3wgFEgBxgmedEdhnH")
    System.setProperty("twitter4j.oauth.accessToken", "1246307037304127489-gsd4LMmqOxCeJMNEqp3Wste8ghb8Mz")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "QDeDZkoqNlzm93lZ9hl9jKW5SpNtJIVCT1xsR5RpPd2M9")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val ds1 = TwitterUtils.createStream(ssc, None)
    val ds2 = ds1.map(x => x.getText).filter(x => x.contains("corona"))

    ds2.count().print()
    ds2.print()
    ds2.saveAsTextFiles("C:\\Users\\narla\\Desktop\\coronaoutput")
    ssc.start()
    ssc.awaitTermination()
  }
}
