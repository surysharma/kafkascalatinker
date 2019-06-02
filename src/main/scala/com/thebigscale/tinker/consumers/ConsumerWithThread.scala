package com.thebigscale.tinker.consumers

import java.time.Duration
import java.util.Properties
import java.util.concurrent.CountDownLatch

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

object ConsumerWithThread extends App {

  // latch for dealing with multiple threads
  //Create properties
  val GROUP_ID = "consumer_group_app_2"
  val TOPIC = "test_topic_1"
  val latch: CountDownLatch  = new CountDownLatch(1);

  val runnable: Runnable = new ConsumerRunnable("127.0.0.1:9092", GROUP_ID, TOPIC, latch)

}

class ConsumerRunnable(bootstrapSever: String, groupId: String, topic: String, latch: CountDownLatch) extends Runnable {

  var consumer: KafkaConsumer[String, String] = _

  val logger = LoggerFactory.getLogger(this.getClass.getName);

  val log = LoggerFactory.getLogger(this.getClass)


  val properties = new Properties()
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapSever)
  properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName())
  properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName())
  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")



  //Create Consumer
  val kafkaConsumer = new KafkaConsumer[String, String](properties)

  //Subscribe to topics
  import scala.collection.JavaConverters._

  kafkaConsumer.subscribe(List(topic).asJavaCollection)


  override def run(): Unit = {
  // poll for new data
    try {
      while (true) {
        val records: ConsumerRecords[String, String] = kafkaConsumer.poll(Duration.ofMillis(100))
        records.forEach(record => log.info(s"key=${record.key()}, value=${record.value()}, partition=${record.partition()}, Offset=${record.offset()}"))

      }
    } catch {
      case _: WakeupException =>
        logger.info("Received shutdown signal!")
    } finally {
      consumer.close()
      // tell our main code we're done with the consumer
      latch.countDown()
    }
}
  def shutdown() {
    // the wakeup() method is a special method to interrupt consumer.poll()
    // it will throw the exception WakeUpException
    consumer.wakeup()
  }
}