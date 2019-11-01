package com.github.aveldannov.kafka.kafkatwitterproject;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {
  //constructor
  public TwitterProducer() {
  }

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run() {
    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

    //create twitter client
    Client client = createTwitterClient(msgQueue);


    // Attempts to establish a connection.
    client.connect();
    //create kafka producer


    // loop to send tweets to kafka


  }

  String consumerKey = "Q361PLWeg45Fk0mXam9KGXziT";
  String consumerSecret = "DyHTgSkFt46mx3nGo8alx14rRzreBYZo2vld37zeRYCvDNpSVP";
  String token = "1017131590571343872-rETtuIpYgrurD8M2MGc3QZJPzXExiK";
  String secret = "8HyVCJjlQiswC20e8kaufzcDjPfVYqjF6uVA2CQ5r2Phd";

  public Client createTwitterClient(BlockingQueue<String> msgQueue) {

    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    // Optional: set up some followings and track terms
    List<String> terms = Lists.newArrayList("kafka");
    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

    ClientBuilder builder = new ClientBuilder()
            .name("Hosebird-Client-01")                              // optional: mainly for the logs
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

    Client hosebirdClient = builder.build();
    return hosebirdClient;


  }

}
