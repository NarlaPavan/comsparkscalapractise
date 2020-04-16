package com.spark

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}

object Producer1 {
  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger("org.apache");
    log.setLevel(Level.INFO)
    val props = new Properties()

    props.put("bootstrap.servers","192.168.172.130:9092")
    props.put("acks","all")
    props.put("client.id","ProducerApp")
    props.put("retries","4")
    props.put("batch.size","32768")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val topic = "topicZ"

    val producer = new KafkaProducer[String,String](props)
    val msg:String  = "Welcome to Kafka"

//    for (i <- 1 to 10){
      val data = new ProducerRecord[String,String](topic,msg)
      producer.send(data)
//    }
    producer.close()
    println("-------------------Sucessfully published message to topic : "+topic+"----------------")
  }
}
