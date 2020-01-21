package streaming.ingest;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;


public class KinesisTwitterSparkStreaming implements Serializable {

    private static final long serialVersionUID = 1L;    
    
    public static void main(String[] args) throws Exception{

        Properties props = new ApplicationConfigMain().getPropValues();
       
        String appName = props.getProperty("appName");
        String master = props.getProperty("master");

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
    
    }

}
