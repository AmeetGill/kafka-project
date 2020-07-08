package com.kafka.app.twitter;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.kafka.app.twitter.dto.TwitterSecrets;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {

    private final Logger logger = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) throws ClientProtocolException, IOException {
        new Producer().run();
    }

    private Producer() {}

    private void safelyCloseApplication(Client twitterClient, KafkaProducer<String,String> kafkaProducer) {
        logger.info("Stopiing application....");
        logger.info("Shutting down twitter Client......");
        twitterClient.stop();
        logger.info("Clsing down Kafka Producer......");
        kafkaProducer.close();
        logger.info("Application stopped...");
    }

    private void run() throws ClientProtocolException, IOException {

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        // create Twitter Client
        Client twitterClient = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        twitterClient.connect();
        // create Producer
        String topic = "twitter_tweets";
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            safelyCloseApplication(twitterClient,kafkaProducer);
        }));
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5,TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                safelyCloseApplication(twitterClient,kafkaProducer);
            }
            if(msg != null && !msg.isEmpty()) {
                send (msg, topic, kafkaProducer);
            }
        }

    }

    private void send(String msg, String topic, KafkaProducer<String,String> kafkaProducer) {
        ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,null,msg);
        sendProducerRecord(record,kafkaProducer);
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) throws ClientProtocolException, IOException {

        /**
         * Declare the host you want to connect to, the endpoint, and authentication
         * (basic auth or oauth)
         */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

        TwitterSecrets twitterSecrets = getTwitterSecret();
        Authentication hosebirdAuth = new OAuth1(
            twitterSecrets.getConsumerKey(),
            twitterSecrets.getConsumerSecret(),
            twitterSecrets.getToken(),
            twitterSecrets.getSecret()
            );

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts).authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    /**
     * 
     * Function to fetch Twiiter Screts from configuration server
     * 
     * @return TwitterSecrets
     * @throws ClientProtocolException
     * @throws IOException
     */
    private  TwitterSecrets getTwitterSecret() throws ClientProtocolException, IOException {
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse closeableHttpResponse = null;
        try{
            httpClient = HttpClients.createDefault();
            HttpGet getRequest = new HttpGet("http://localhost:3000/secret/twitter");
            closeableHttpResponse = httpClient.execute(getRequest);
            String response = EntityUtils.toString(closeableHttpResponse.getEntity());
            return new Gson().fromJson(response, TwitterSecrets.class);
        } finally {
            logger.error("Not able to fetch Twitter Secrets");
            if(closeableHttpResponse != null) closeableHttpResponse.close();
            if(httpClient != null) httpClient.close();
        }

    }

    private KafkaProducer<String,String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String,String>(properties);
    }

    private void sendProducerRecord(ProducerRecord<String,String> producerRecord,
            KafkaProducer<String,String> kafkaProducer) {
        kafkaProducer.send(producerRecord,new Callback(){
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception == null) {
                    logger.info("Recieved new metadata. \n" +
                                "Topic:" + metadata.topic() + "\n" +
                                "Partition" + metadata.partition() + "\n" +
                                "Offset:" + metadata.offset() + "\n" +
                                "Timestamp:" + metadata.timestamp());
                } else {
                    logger.error("Error while producing",exception);
                }
            }
        });
    } 
}