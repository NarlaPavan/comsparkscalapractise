package com.spark

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger


object Producer01 {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    val topic = "topicC"
//    val log = Logger.getLogger("org.apache")
    props.put("bootstrap.servers","192.168.172.130:9092")
    props.put("client.id","KafkaProducer")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](props)
    val msg = "Welcome to kafka"
    for (i <- 1 to 10){
      val data = new ProducerRecord[String,String](topic,msg)
      producer.send(data)
    }
    producer.close()
    println("----Sucessfully published message to topic: "+topic+" -------")

  }
}
