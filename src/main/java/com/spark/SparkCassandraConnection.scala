package com.spark

import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._

object SparkCassandraConnection {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Cassandra Connection").master(("local")).getOrCreate()

    val rd1 = spark.sparkContext.cassandraTable("cassandrakeyspace1","employee")

//    rd1.collect.foreach(println)
    val rd2_tuple = spark.sparkContext.cassandraTable[(Int,Option[Int],Option[String],Option[String])]("cassandrakeyspace1","employee")

    val df1 = spark.createDataFrame(rd2_tuple)
//    df1.show()

    val rd3_tuple = spark.sparkContext.cassandraTable[Employee]("cassandrakeyspace1","employee")

    val df2 = spark.createDataFrame(rd3_tuple)
    df2.show()
    val deptDf = df2.select("dept").na.fill("NO DEPT")

    deptDf.show()
    df2.na.drop().show()

    val df3 = spark.read.option("multiLine","true").json("C:\\Users\\narla\\IdeaProjects\\comsparkscalapractise\\src\\main\\resources\\jsonfile")

    val ob1 = new Employee(105,33,Option("it"),Option("Pavan Kumar"))
    val rd4 = spark.sparkContext.makeRDD(Seq(ob1))
    rd4.saveToCassandra("cassandrakeyspace1","employee")
    spark.close()
  }
}
