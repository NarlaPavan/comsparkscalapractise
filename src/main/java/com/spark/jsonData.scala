package com.spark

import org.apache.spark.sql.SparkSession

object jsonData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Json Data").master("local").getOrCreate()

        val jsonDf = spark.read.option("multiLine","true").json(
          "C:\\Users\\narla\\IdeaProjects\\comsparkscalapractise\\src\\main\\resources\\jsonfile",
          "C:\\Users\\narla\\IdeaProjects\\comsparkscalapractise\\src\\main\\resources\\jsonfile1"
        )
//    val data = spark.sparkContext.wholeTextFiles("C:\\Users\\narla\\IdeaProjects\\comsparkscalapractise\\src\\main\\resources\\jsonfile").values
//    val jsonDf = spark.read.json(data)
    jsonDf.show()
    spark.close()
  }
}
