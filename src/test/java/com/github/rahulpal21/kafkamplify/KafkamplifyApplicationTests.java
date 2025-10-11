package com.github.rahulpal21.kafkamplify;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.random.RandomGenerator;

@SpringBootTest
@EmbeddedKafka(partitions = 2, topics = {"test-topic"})
class KafkamplifyApplicationTests {

    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;

    @BeforeEach
    public void setup() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Create Kafka template
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
        RandomGenerator randomGenerator = new Random();
        System.out.println("*****************   PRELOADING  **********************");
        long before = System.currentTimeMillis();
        for (int i = 0; i < 100000000; i++) {
            kafkaTemplate.send("test-topic", String.valueOf(i), String.valueOf(randomGenerator.nextLong()));
        }
        System.out.println(System.currentTimeMillis()-before);
        System.out.println("*****************   FINISHED PRELOADING  **********************");

    }

    @Test
    void contextLoads() throws InterruptedException {
        Thread.currentThread().sleep(Integer.MAX_VALUE);
    }

    @AfterAll
    public static void cleanup() {

    }

}