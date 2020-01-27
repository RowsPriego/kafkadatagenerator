package com.tree.rows.multikafkaproducer.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;

import com.google.gson.Gson;
import com.tree.rows.multikafkaproducer.MyKafkaProducer;
import com.tree.rows.multikafkaproducer.MyKafkaProducerInterface;
import com.tree.rows.multikafkaproducer.config.ApplicationConfigMain;
import com.tree.rows.multikafkaproducer.config.TwitterConfig;
import com.tree.rows.multikafkaproducer.twitter.model.Tweet;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterKafkaProducer implements MyKafkaProducerInterface {

    private static final String TOPIC = "tweets";

    private Client client;
    private BlockingQueue<String> queue;
    private Gson gson;
    private TwitterConfig twitterConfig;
    private MyKafkaProducer myTwitterProducer;

    public TwitterKafkaProducer() throws IOException {

        Properties appProps = new ApplicationConfigMain().getPropValues();
        
        myTwitterProducer = new MyKafkaProducer.Builder()
            .setKafkaBrokers(appProps.getProperty("kafkaServers"))
            .setKafkaTopic(TOPIC)
            .build();

        twitterConfig = new TwitterConfig();

        Authentication auth = new OAuth1(twitterConfig.getTwitterConsumerKey(),
                twitterConfig.getTwitterConsumerSecret(), twitterConfig.getTwitterAccessToken(),
                twitterConfig.getTwitterAccessTokenSecret());
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Collections.singletonList(twitterConfig.getTwitterHashtag()));

        queue = new LinkedBlockingQueue<>(100);

        client = new ClientBuilder().hosts(Constants.STREAM_HOST).authentication(auth).endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue)).build();

        gson = new Gson();
    }
    
    public void run() {
        client.connect();       

        try (Producer<Long, String> producer = myTwitterProducer.getProducer()) {
            while (true) {
                Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
                System.out.printf("Fetched tweet id %d\n", tweet.getId());
                long key = tweet.getId();
                String msg = tweet.toString();

                ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, key, msg);
                producer.send(record, myTwitterProducer.getCallback());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }

    }

}