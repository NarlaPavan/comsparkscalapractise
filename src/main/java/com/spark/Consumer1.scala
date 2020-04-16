package com.spark

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConversions._

object Consumer1 {
  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger("org.apache");
    log.setLevel(Level.INFO)
    val props = new Properties()

    props.put("bootstrap.servers", "192.168.172.130:9093")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "testgroup")
    props.put("heartbeat.interval.ms", "4000")
    props.put("client.id", "ConsumerApp")

    val consumer = new KafkaConsumer[String, String](props)
    val topic = "topicZ"
    consumer.subscribe(Collections.singletonList(topic))

    while (true) {
      val records = consumer.poll(100)
      for (record <- records.iterator()) {
        println("Received Message " + record)
      }
    }
    //    consumer.commitAsync()
    //    println("----Consumer Received Message from topic " + topic + "-----")
  }
}
