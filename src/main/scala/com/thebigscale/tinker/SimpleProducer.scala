package com.thebigscale.tinker

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object SimpleProducer extends App {

  //Create properties
  val properties = new Properties()
  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName())
  properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName())
  properties.setProperty(ProducerConfig.ACKS_CONFIG, "1")

  //Create producer
  val kafkaProducer = new KafkaProducer[String, String](properties)


  for (i <- 1 to 10) {
    //Create producer record
    val producerRecord = new ProducerRecord[String, String]("test_topic_1", s"Hello kafka World $i")
    //Send record
    kafkaProducer.send(producerRecord)
  }


  //Flush and Close
  kafkaProducer.flush()
  kafkaProducer.close()

}
