package com.tree.rows.multikafkaproducer;

import java.io.Serializable;

import com.tree.rows.multikafkaproducer.random.RamdonKafkaProducer;
import com.tree.rows.multikafkaproducer.twitter.TwitterKafkaProducer;

public class KafkaDataProducerApp implements Serializable {

    private static final long serialVersionUID = 1L;
    

    public static void main(String[] args) throws Exception {

        
        System.out.println("Lets produce some data!");

        MyKafkaProducerInterface producer;

        if (args.length > 0) {
            String producerType  = args[0];
            if (producerType.equals("random")) {
                producer = new RamdonKafkaProducer();            
            } else {
                producer = new TwitterKafkaProducer();
            }
            producer.run();  
        } else {
            System.out.println("[producerType] parameter is mandatory. Please select: random, twitter, file ...");
        }                   
        
                    

    }

}
