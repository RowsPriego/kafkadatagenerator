package com.tree.rows.multikafkaproducer.config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class ApplicationConfigMain {
    
    InputStream inputStream;
    final private static String PROPERTIES_FILE_PATH = "config.properties";
    final private Properties prop = new Properties();
    
    public Properties getPropValues() throws IOException {  

    try {
        
        String propFileName = "config.properties";

        inputStream = getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE_PATH);

        if (inputStream != null) {
            prop.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }
       
    } catch (Exception e) {
        System.out.println("Exception: " + e);
    } finally {
        inputStream.close();
    }
    return prop;
    

    }
}