package streaming.ingest;

import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import streaming.ingest.model.RamdonJson;

import java.io.IOException;


public class RamdonKafkaProducer {  
    
    
    private Callback callback;
    private Properties props;
    private String topic;

    public RamdonKafkaProducer() throws IOException {

        props = new ApplicationConfigMain().getPropValues();
        topic = props.getProperty("kafkaTopicRamdon");       
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
        

        try (Producer<Long, String> producer = getProducer()) {
            while (true) {               
                
                long key = new Random().nextLong();                
                String msg = new RamdonJson("Rows"+key, key, "desc for " + key).toString();
                
                ProducerRecord<Long, String> record = new ProducerRecord<>(topic, key, msg);
                producer.send(record, callback);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            
        }

    }

}