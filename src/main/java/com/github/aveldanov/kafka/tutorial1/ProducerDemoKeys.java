package com.github.aveldanov.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

  public static void main(String[] args) throws
          ExecutionException,
          InterruptedException {
    final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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


    for (int i = 0; i < 10; i++) {
      //create a producer record
      String topic = "first_topic";
      String value = "Hello World " + Integer.toString(i);
      String key = "id_" + Integer.toString(i);


      // key - String, value - String
      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

      logger.info("Key: "+key);


      // send data - async
      producer.send(record, new Callback() {
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          // executes every time record is succcessfully sent or an execption is thrown
          if (e == null) {
            //record was successfully sent
            logger.info("Received new metadata. \n" +
                    "Topic:" + recordMetadata.topic() + "\n" +
                    "Partition:" + recordMetadata.partition() + "\n" +
                    "Offset: " + recordMetadata.offset() + "\n" +
                    "Timestamp: " + recordMetadata.timestamp());

          } else {
            logger.error("Error while producing: ", e);
          }
        }
      }).get(); // block the .send() to make it sync - dont do this in production


    }
    //flush data - wait for async data - buffers output
    producer.flush();
    // flush and close producer
    producer.close();

  }


}
