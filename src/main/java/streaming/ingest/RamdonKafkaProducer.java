package streaming.ingest;


import java.util.Random;
import org.apache.kafka.clients.producer.*;

import streaming.ingest.model.RamdonJson;

public class RamdonKafkaProducer extends MyKafkaProducer {    
    
    private String topic;

    public RamdonKafkaProducer() {
        topic = getProps().getProperty("topic");
    }

    @Override
    public void run() {        

        try (Producer<Long, String> producer = getProducer()) {
            while (true) {               
                
                long key = new Random().nextLong();                
                String msg = new RamdonJson("Rows"+key, key, "desc for " + key).toString();
                
                ProducerRecord<Long, String> record = new ProducerRecord<>(topic, key, msg);
                producer.send(record, getCallback());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            
        }

    }

}