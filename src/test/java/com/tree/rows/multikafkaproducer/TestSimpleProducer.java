package com.tree.rows.multikafkaproducer;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Properties;

import com.tree.rows.multikafkaproducer.config.ApplicationConfigMain;

import org.junit.Test;

public class TestSimpleProducer {

    @Test
    public void testCreation() {

        try {
            Properties appProps = new ApplicationConfigMain().getPropValues();
            String TOPIC = "test";        
            String kafkaBrokers = appProps.getProperty("kafkaServers");
            MyKafkaProducer myProducer = new MyKafkaProducer.Builder().setKafkaBrokers(kafkaBrokers).setKafkaTopic(TOPIC).build();

            assertNotNull(myProducer);
        } catch (IOException e) {
            System.out.println("Error al leer las propiedades");
        }
        
    }
}