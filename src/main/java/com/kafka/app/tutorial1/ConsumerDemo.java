package com.kafka.app.tutorial1;

import java.io.Console;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConsumerDemo {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fourth-application";

        Properties properties = new Properties();
    
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String,String>(properties);

        //subscribe consumer to our topic(s)
        kafkaConsumer.subscribe(Arrays.asList("first_topic"));

        while(true) {
            ConsumerRecords<String,String> records = 
                    kafkaConsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord record: records) {
                logger.info("key: {} Value: {} " , record.key(),record.value());
                logger.info("Partition: {} Offset: {}", record.partition(),record.offset());
            }
        }

    }

}