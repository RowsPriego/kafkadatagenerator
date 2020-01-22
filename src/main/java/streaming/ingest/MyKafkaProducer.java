package streaming.ingest;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

public class MyKafkaProducer {

    private Callback callback;    
    private Properties props;
    
    public MyKafkaProducer() {

        try {
            props = new ApplicationConfigMain().getPropValues();
        } catch (IOException e) {            
            e.printStackTrace();
        }
        
        callback = new BasicCallback();
    }


    public Producer<Long, String> getProducer() {

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
    }

    public Callback getCallback() {
        return this.callback;
    }

    public void setCallback(Callback callback) {
        this.callback = callback;
    }

    public Properties getProps() {
        return this.props;
    }

    public void setProps(Properties props) {
        this.props = props;
    }


}