package com.github.rahulpal21.kafkamplify.api;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KafkamplifyMessageHandler<K, V> {
    void handleMessage(ConsumerRecord<K, V> message);
}
