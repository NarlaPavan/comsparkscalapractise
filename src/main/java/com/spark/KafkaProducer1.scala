package com.spark

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer1 {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers","192.168.172.131:9092")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    props.put("batch.size","32768")

    val topic_INFO = "INFO_TOPIC"
    val topic_WARN = "WARN_TOPIC"
    val topic_OTH = "OTH_TOPIC"

    val producer = new KafkaProducer[String,String](props)
    val file = scala.io.Source.fromFile("C:\\Users\\narla\\Desktop\\logs.txt")

    for (line <- file.getLines()){
      val type_pattern = "(INFO|WARN)".r
      val type_ = type_pattern.findFirstIn(line).get

      if(type_ == "INFO" ){
        val msg = new ProducerRecord[String,String](topic_INFO,line)
        producer.send(msg)
      }else if(type_ == "WARN"){
        val msg = new ProducerRecord[String,String](topic_WARN,line)
        producer.send(msg)
      }else{
        val msg = new ProducerRecord[String,String](topic_OTH,line)
        producer.send(msg)
      }
    }

    producer.close()
  }
}
