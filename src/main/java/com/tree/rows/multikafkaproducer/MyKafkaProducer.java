package com.tree.rows.multikafkaproducer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyKafkaProducer {

    private Callback callback;
    private Properties kafkaProps;
    private String topic;
    private String kafkaBrokers;
    private String registryBrokers;

    public MyKafkaProducer() {
        
    }

    public static class Builder {
        private MyKafkaProducer myproducer;

        public Builder() {
            myproducer = new MyKafkaProducer();
        }

        public Builder setKafkaBrokers(String kafkabBrokers) {
            myproducer.setKafkaBrokers(kafkabBrokers);

            return this;
        }

        public Builder setKafkaTopic(String kafkaTopic) {
            myproducer.setTopic(kafkaTopic);

            return this;
        }

        public Builder setKafkaProperties(Properties kafkaProperties) {
            myproducer.setKafkaProps(kafkaProperties);

            return this;
        }

        public Builder setRegistryBrokers(String registryBrokers) {
            myproducer.setRegistryBrokers(registryBrokers);

            return this;
        }

        public MyKafkaProducer build() {

            if (null == myproducer.getKafkaBrokers()) {
                throw new IllegalStateException("Invalid initialization: kafka brokers required");
            }

            if (null == myproducer.getTopic()) {
                throw new IllegalStateException("Invalid initialization: kafka topic required");
            }

            myproducer.callback = new BasicCallback();
            
            Properties kafkaProps = new Properties();
            kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, myproducer.getKafkaBrokers());
            kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
            kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, 500);
            kafkaProps.put(ProducerConfig.RETRIES_CONFIG, 0);
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            myproducer.setKafkaProps(kafkaProps);

            return myproducer;

        }
    }    

    public Producer<Long, String> getProducer() {

        return new KafkaProducer<>(this.kafkaProps);
    }

    public Callback getCallback() {
        return this.callback;
    }

    public String getTopic() {
        return this.topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Properties getKafkaProps() {
        return this.kafkaProps;
    }

    public void setKafkaProps(Properties kafkaProps) {
        this.kafkaProps = kafkaProps;
    }

    public String getKafkaBrokers() {
        return this.kafkaBrokers;
    }

    public void setKafkaBrokers(String kafkaBrokers) {
        this.kafkaBrokers = kafkaBrokers;
    }

    public String getRegistryBrokers() {
        return this.registryBrokers;
    }

    public void setRegistryBrokers(String registryBrokers) {
        this.registryBrokers = registryBrokers;
    }


}