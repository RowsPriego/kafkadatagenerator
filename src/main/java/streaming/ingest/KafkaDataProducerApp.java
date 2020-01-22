package streaming.ingest;

import java.io.Serializable;

public class KafkaDataProducerApp implements Serializable {

    private static final long serialVersionUID = 1L;
    

    public static void main(String[] args) throws Exception {

        String producerType = "twitter"; //Twitter by default
        MyKafkaProducer producer = new MyKafkaProducer();
        System.out.println("Lets produce some data!");

        if (args.length > 0) {
            producerType  = args[0];
        }

        if (producerType.equals("random")) {
            producer = new RamdonKafkaProducer();            
        } else {
            producer = new TwitterKafkaProducer();
        }              
        
        producer.run();              

    }

}
