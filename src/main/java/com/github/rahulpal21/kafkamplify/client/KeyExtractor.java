package com.github.rahulpal21.kafkamplify.client;

import com.github.rahulpal21.kafkamplify.KafkamplifyKeyExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

//@Component
public class KeyExtractor implements KafkamplifyKeyExtractor {
    @Override
    public String extractKey(ConsumerRecord record) {
        return "";
    }
}
