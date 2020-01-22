package streaming.ingest;

import java.io.Serializable;

public class KafkaDataProducerApp implements Serializable {

    private static final long serialVersionUID = 1L;
    

    public static void main(String[] args) throws Exception {

        MyKafkaProducer producer = new MyKafkaProducer();
        System.out.println("Lets produce some data!");

        if (args.length > 0) {
            String producerType  = args[0];
            if (producerType.equals("random")) {
                producer = new RamdonKafkaProducer();            
            } else {
                producer = new TwitterKafkaProducer();
            }
            producer.run();  
        } else {
            System.out.println("[producerType] parameter is mandatory. Please select: random, twitter");
        }                   
        
                    

    }

}
