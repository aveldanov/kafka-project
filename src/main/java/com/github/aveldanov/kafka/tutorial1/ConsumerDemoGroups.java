package com.github.aveldanov.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {
  public static void main(String[] args) {

    Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());
    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "my-sixth-application";
    String topic = "first_topic";
    logger.info("HI PEOPLE");

    // Create Consumer Configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

    // AUTO_OFFSET_RESET - "earliest, latest, none "
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    //Create a consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    //Subscribe consumer to our topic
    // Signleton - subsctibe to one topic
    //    consumer.subscribe(Collections.singleton(topic));
    consumer.subscribe(Arrays.asList(topic));


    // poll for new data
    // need to force Java8 usage for the feature (Duration part)
    while (true) {
      ConsumerRecords<String, String> records =
              consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0
      for (ConsumerRecord<String, String> record : records) {
        logger.info("Key: " + record.key() + ", Value: " + record.value());
        logger.info("Partition " + record.partition() + ", Offset" + record.offset());
      }


    }


  }
}
