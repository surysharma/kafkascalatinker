package com.thebigscale.tinker.consumers

import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

object SimpleConsumer extends App {

  val log = LoggerFactory.getLogger(this.getClass)

  //Create properties
  val GROUP_ID="consumer_group_app_1"

  val properties = new Properties()
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName())
  properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName())
  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID)
  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  //Create Consumer
  val kafkaConsumer = new KafkaConsumer[String, String](properties)

  //Subscribe to topics
  import scala.collection.JavaConverters._
  kafkaConsumer.subscribe(List("test_topic_1").asJavaCollection)

  while(true) {
    val records: ConsumerRecords[String, String] = kafkaConsumer.poll(Duration.ofMillis(100))
    records.forEach(record => log.info(s"key=${record.key()}, value=${record.value()}, partition=${record.partition()}, Offset=${record.offset()}"))
  }
}
