package streaming.ingest;

import java.util.Properties;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import streaming.ingest.model.Tweet;

import com.google.gson.Gson;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterKafkaProducer {

    private Client client;
    private BlockingQueue<String> queue;
    private Gson gson;
    private Callback callback;
    private Properties props;
    private String topic;

    public TwitterKafkaProducer() throws IOException {

        props = new ApplicationConfigMain().getPropValues();

        String consumerKey = props.getProperty("twitterConsumerKey");
        String consumerSecret = props.getProperty("twitterConsumerSecret");
        String accessToken = props.getProperty("twitterAccessToken");
        String accessTokenSecret = props.getProperty("twitterAccessTokenSecret");
        String hashtag = props.getProperty("hashtag");
        topic = props.getProperty("kafkaTopicTweets");

        Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Collections.singletonList(hashtag));

        queue = new LinkedBlockingQueue<>(100);

        client = new ClientBuilder().hosts(Constants.STREAM_HOST).authentication(auth).endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue)).build();

        gson = new Gson();
        callback = new BasicCallback();
    }

    private Producer<Long, String> getProducer() {

        String bootstrapServers = props.getProperty("kafkaServers");

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    public void run() {
        client.connect();

        try (Producer<Long, String> producer = getProducer()) {
            while (true) {
                Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
                System.out.printf("Fetched tweet id %d\n", tweet.getId());
                long key = tweet.getId();
                String msg = tweet.toString();
               
                ProducerRecord<Long, String> record = new ProducerRecord<>(topic, key, msg);
                producer.send(record, callback);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }

    }

}