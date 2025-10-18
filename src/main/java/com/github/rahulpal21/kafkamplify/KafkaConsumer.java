package com.github.rahulpal21.kafkamplify;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

@Slf4j
@Service
public class KafkaConsumer {
    private ForkJoinPool pool = new ForkJoinPool(20, new KafkamplifyThreadFactory(), (t, e) -> {
        System.out.println(t.toString() + e.getMessage());
    }, true);

    private Map<String, KafkamplifyRecursiveTask<String, String>> taskContainer = new ConcurrentHashMap<>();
    private KafkamplifyKeyExtractor keyExtractor;
    private final LongCounter batchCount = GlobalOpenTelemetry.get().meterBuilder("").build().counterBuilder("batchCount").build();
    private final LongHistogram batchLatency = GlobalOpenTelemetry.get().meterBuilder("").build().histogramBuilder("batchCount").ofLongs().build();

    @KafkaListener(id = "defaultlistener",
            topics = {"test-topic"}, batch = "true")
    public void listen(List<ConsumerRecord<String, String>> records) {
        long before = System.nanoTime();

        batchCount.add(records.size());

        List<ForkJoinTask<String>> tasks = new ArrayList<>();
        records.forEach(record -> {
            tasks.add(submitTask(record));
        });

        log.trace("******************** ALL SUBMITTED ******************");
        tasks.forEach(stringForkJoinTask -> {
            stringForkJoinTask.join();
            batchLatency.record(System.nanoTime()-before);
        });
        log.trace("******************* ALL PROCESSED **********************");
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
