package com.github.rahulpal21.kafkamplify;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

@Service
public class KafkaConsumer {
    private ForkJoinPool pool = new ForkJoinPool(20, new KafkamplifyThreadFactory(), (t, e) -> {
        System.out.println(t.toString() + e.getMessage());
    }, true);

    private Map<String, KafkamplifyRecursiveTask<String, String>> taskContainer = new ConcurrentHashMap<>();
    private KafkamplifyKeyExtractor keyExtractor;

    @KafkaListener(id = "defaultlistener",
            topics = {"test-topic"}, batch = "true")
    public void listen(List<ConsumerRecord<String, String>> records) {
        System.out.println(records.size());

        List<ForkJoinTask<String>> tasks = new ArrayList<>();
        records.forEach(record -> {
            tasks.add(submitTask(record));
        });

        System.out.println("******************** ALL SUBMITTED ******************");
        tasks.forEach(stringForkJoinTask -> {
            stringForkJoinTask.join();
        });
        System.out.println("******************* ALL PROCESSED **********************");
    }

    private ForkJoinTask<String> submitTask(ConsumerRecord<String, String> record) {
        String key = keyExtractor.extractKey(record);
        KafkamplifyRecursiveTask<String, String> task1 = taskContainer.get(key);
        if (task1 == null) {
            task1 = new KafkamplifyRecursiveTask<>(record);
            taskContainer.put(key, task1);
            return pool.submit(task1);
        } else {
            try {
                task1.enqueue(record);
                return task1;
            } catch (QueueClosedException e) {
                task1 = new KafkamplifyRecursiveTask<>(record);
                taskContainer.put(key, task1);
                return pool.submit(task1);
            }
        }
    }
}
