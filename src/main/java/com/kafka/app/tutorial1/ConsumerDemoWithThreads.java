package com.kafka.app.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThreads {
    private final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();

    }

    private ConsumerDemoWithThreads() {

    }

    private void run() {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";

        final CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating consumer thread");
        final Runnable myConsumerRunnable = new ConsumerRunnable(latch, bootstrapServers, groupId, topic);

        Thread newThread = new Thread(myConsumerRunnable);
        newThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application Interuppted",e);
        } finally {
            logger.info("Application is closing");
        }




    }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;

        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(CountDownLatch latch, String bootstrapServers, String groupId, String topic) {
            this.latch = latch;

            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            this.consumer = new KafkaConsumer<String, String>(properties);

            // subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));

        }

        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String,String> record : records) {
                        logger.info("key: {} Value: {} ", record.key(), record.value());
                        logger.info("Partition: {} Offset: {}", record.partition(), record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Recieved Shutdown signal");
            } finally {
                this.consumer.close();
                this.latch.countDown();
            }
        }

        public void shutdown() {
            this.consumer.wakeup();
        }
    }
}

