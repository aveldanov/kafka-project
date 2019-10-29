package com.github.aveldanov.kafka.tutorial1;

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

    properties.setProperty("bootstarp.servers", bootstrapServers);
    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());




    // create the producer


    // send data


  }


}
