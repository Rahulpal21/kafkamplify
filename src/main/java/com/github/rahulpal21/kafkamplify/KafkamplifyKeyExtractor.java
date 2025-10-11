package com.github.rahulpal21.kafkamplify;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KafkamplifyKeyExtractor<K, V> {
    String extractKey(ConsumerRecord<K, V> record);
}
