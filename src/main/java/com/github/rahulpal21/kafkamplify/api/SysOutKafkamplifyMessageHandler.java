package com.github.rahulpal21.kafkamplify.api;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

@Component
public class SysOutKafkamplifyMessageHandler<K, V> implements KafkamplifyMessageHandler<K, V> {

    @Override
    public void handleMessage(ConsumerRecord<K, V> message) {
        if (message == null) {
            System.out.println("Received null message");
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Received message - topic:").append(message.topic())
          .append(", partition:").append(message.partition())
          .append(", offset:").append(message.offset())
          .append(", key:").append(message.key())
          .append(", value:").append(message.value());
        System.out.println(sb.toString());
    }
}

