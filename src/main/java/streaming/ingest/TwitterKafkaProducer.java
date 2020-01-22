package streaming.ingest;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;

import streaming.ingest.model.Tweet;

import com.google.gson.Gson;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterKafkaProducer extends MyKafkaProducer {

    private Client client;
    private BlockingQueue<String> queue;
    private Gson gson;

    public TwitterKafkaProducer() throws IOException {
        

        String consumerKey = getProps().getProperty("twitterConsumerKey");
        String consumerSecret = getProps().getProperty("twitterConsumerSecret");
        String accessToken = getProps().getProperty("twitterAccessToken");
        String accessTokenSecret = getProps().getProperty("twitterAccessTokenSecret");
        String hashtag = getProps().getProperty("hashtag");
        

        Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Collections.singletonList(hashtag));

        queue = new LinkedBlockingQueue<>(100);

        client = new ClientBuilder().hosts(Constants.STREAM_HOST).authentication(auth).endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue)).build();

        gson = new Gson();        
    }

    
    @Override
    public void run() {
        client.connect();

        String topic = getProps().getProperty("topic");

        try (Producer<Long, String> producer = getProducer()) {
            while (true) {
                Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
                System.out.printf("Fetched tweet id %d\n", tweet.getId());
                long key = tweet.getId();
                String msg = tweet.toString();
               
                ProducerRecord<Long, String> record = new ProducerRecord<>(topic, key, msg);
                producer.send(record, getCallback());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }

    }

}