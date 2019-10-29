package com.github.aveldanov.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

  public static void main(String[] args) {
    Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallback.class);

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
    final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello World");

    // send data - async


    producer.send(record, new Callback() {
      public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        // executes every time record is succcessfully sent or an execption is thrown
      if(e!=null){
        //record was successfully sent

      }
      else{
        recordMetadata.
      }
      }
    });
    //flush data - wait for async data - buffers output
    producer.flush();
    // flush and close producer
    producer.close();

  }


}
