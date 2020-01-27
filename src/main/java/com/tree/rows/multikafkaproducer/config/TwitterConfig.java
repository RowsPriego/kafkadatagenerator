package com.tree.rows.multikafkaproducer.config;

import java.io.IOException;
import java.util.Properties;

public class TwitterConfig {
    private String twitterConsumerKey;
    private String twitterConsumerSecret;
    private String twitterAccessToken;
    private String twitterAccessTokenSecret;
    private String twitterHashtag;
    
    public TwitterConfig() {
        try {
            Properties appProps = new ApplicationConfigMain().getPropValues();
            this.twitterConsumerKey = appProps.getProperty("twitterConsumerKey");
            this.twitterConsumerSecret = appProps.getProperty("twitterConsumerSecret");
            this.twitterAccessToken = appProps.getProperty("twitterAccessToken");
            this.twitterAccessTokenSecret = appProps.getProperty("twitterAccessTokenSecret");
            this.twitterHashtag = appProps.getProperty("twitterHashtag");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public String getTwitterConsumerKey() {
        return this.twitterConsumerKey;
    }

    public void setTwitterConsumerKey(String twitterConsumerKey) {
        this.twitterConsumerKey = twitterConsumerKey;
    }

    public String getTwitterConsumerSecret() {
        return this.twitterConsumerSecret;
    }

    public void setTwitterConsumerSecret(String twitterConsumerSecret) {
        this.twitterConsumerSecret = twitterConsumerSecret;
    }

    public String getTwitterAccessToken() {
        return this.twitterAccessToken;
    }

    public void setTwitterAccessToken(String twitterAccessToken) {
        this.twitterAccessToken = twitterAccessToken;
    }

    public String getTwitterAccessTokenSecret() {
        return this.twitterAccessTokenSecret;
    }

    public void setTwitterAccessTokenSecret(String twitterAccessTokenSecret) {
        this.twitterAccessTokenSecret = twitterAccessTokenSecret;
    }

    public String getTwitterHashtag() {
        return this.twitterHashtag;
    }

    public void setTwitterHashtag(String twitterHashtag) {
        this.twitterHashtag = twitterHashtag;
    }
}