package com.kafka.app.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProducerDemoKeys {

    public static void main(String args[]) throws InterruptedException, ExecutionException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        String bootstrapServers = "127.0.0.1:9092";
        
        //Define properties for kafka
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);
        for(int i = 0; i < 10; i++){

            String topic = "first_topic";
            String value = "hello_world" + i;
            String key = "id_" + i;

            ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,key,value);
            
            logger.info("Key: "+key);

            producer.send(record,new Callback(){
            
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
            }).get();
        }

        producer.close();
    }

}