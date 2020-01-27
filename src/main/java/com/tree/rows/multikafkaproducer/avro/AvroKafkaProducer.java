package com.tree.rows.multikafkaproducer.avro;

import java.io.IOException;
import java.util.Properties;

import com.tree.rows.multikafkaproducer.MyKafkaProducer;
import com.tree.rows.multikafkaproducer.MyKafkaProducerInterface;
import com.tree.rows.multikafkaproducer.config.ApplicationConfigMain;

public class AvroKafkaProducer implements MyKafkaProducerInterface {

    private static final String TOPIC = "avrotest";
    private MyKafkaProducer myAvroProducer;

    public AvroKafkaProducer() throws IOException {

        Properties appProps = new ApplicationConfigMain().getPropValues();
        myAvroProducer = new MyKafkaProducer.Builder()
            .setKafkaBrokers(appProps.getProperty("kafkaServers"))
            .setKafkaTopic(TOPIC)
            .setRegistryBrokers(appProps.getProperty("registryServers"))
            .build();

    }

    @Override
    public void run() {
        // TODO Auto-generated method stub

    }
    
}