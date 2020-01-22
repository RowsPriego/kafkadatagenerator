package streaming.ingest;

import java.io.Serializable;

public class Twitter2KafkaApp implements Serializable {

    private static final long serialVersionUID = 1L;

    public static void main(String[] args) throws Exception {

        System.out.println("Lets Consume some twits!");
         TwitterKafkaProducer producer = new TwitterKafkaProducer();
        //RamdonKafkaProducer producer = new RamdonKafkaProducer();
        producer.run();              

    }

}
