package com.tree.rows.multikafkaproducer.random;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import com.github.jsontemplate.JsonTemplate;
import com.tree.rows.multikafkaproducer.MyKafkaProducer;
import com.tree.rows.multikafkaproducer.MyKafkaProducerInterface;
import com.tree.rows.multikafkaproducer.config.ApplicationConfigMain;

import org.apache.kafka.clients.producer.*;

public class RamdonKafkaProducer implements MyKafkaProducerInterface {

    private static final String TOPIC = "test";
    private MyKafkaProducer myRandomProducer;
    private String jsonTemplate;

    public RamdonKafkaProducer() throws IOException {

        Properties appProps = new ApplicationConfigMain().getPropValues();

        myRandomProducer = new MyKafkaProducer.Builder()
            .setKafkaBrokers(appProps.getProperty("kafkaServers"))
            .setKafkaTopic(TOPIC)
            .build();
    
    }
    
    public void run() {        

        try (Producer<Long, String> producer = myRandomProducer.getProducer()) {
            while (true) {               
                
                long key = new Random().nextLong();                
                String template = "{" +
                  "  name : @s," +
                  "  age : @i(max=100)," +
                  "  role : @s[](min=1, max=3)," +
                  "  address : {" +
                  "    city : @s," +
                  "    street : @s," +
                  "    number : @i" +
                  "  }" +
                  "}";
                String json = new JsonTemplate(template).prettyString();  
                
                ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, key, json);
                producer.send(record, myRandomProducer.getCallback());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            
        }
    }

    public String getJsonTemplate() {
        return this.jsonTemplate;
    }

    public void setJsonTemplate(String jsonTemplate) {
        this.jsonTemplate = jsonTemplate;
    }

}