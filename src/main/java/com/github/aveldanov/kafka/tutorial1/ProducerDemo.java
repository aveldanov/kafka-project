package com.github.aveldanov.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

  public static void main(String[] args) {
    String bootstrapServers = "127.0.0.1:9092";
    // create Producer properties
    Properties properties = new Properties();
    //    properties.setProperty("bootstarp.servers", bootstrapServers);
    //    properties.setProperty("key.serializer", StringSerializer.class.getName());
    //    properties.setProperty("value.serializer", StringSerializer.class.getName());

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the producer
    // key - String, value - String
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    //create a producer record
    // key - String, value - String
    ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello World");

    // send data


    producer.send(record);


  }


}
