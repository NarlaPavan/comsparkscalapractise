package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CaseStudy1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Case Study1").master("local").getOrCreate()


    val rd1 = spark.sparkContext.textFile("C:\\Users\\narla\\IdeaProjects\\comsparkscalapractise\\src\\main\\resources\\ad_event")

    val patrn = "\"[A-Za-z0-9_-]+\": \\{".r


    val rd3 = rd1.flatMap { x =>
      x.split("\"[A-Za-z0-9_-]+\": \\{")
    }
    val rd4 = rd3.map(x => {

      if (x.equalsIgnoreCase("{")) {
        " "
      } else if ((x.count(_ == '}') == 2)) {
        val data = x.replace("\"viewCount\"", "{\"viewCount\"")
        val data1 = data.replace("}}", "}")
        data1
      } else {
        val data = x.replace("\"viewCount\"", "{\"viewCount\"")
        data
      }

    })

    //    rd4.saveAsTextFile("C:\\Users\\narla\\Desktop\\joutput")
    //    val rdd1 = spark.sparkContext.wholeTextFiles("C:\\Users\\narla\\Desktop\\joutput\\").values
    val df1 = spark.read.json("C:\\Users\\narla\\Desktop\\joutput\\")

    df1.show(10)
    df1.printSchema()

    val df2 = df1.select("*").groupBy("categoryId").agg(sum("viewCount"))
    df2.show(40)
    spark.close()
  }
}
