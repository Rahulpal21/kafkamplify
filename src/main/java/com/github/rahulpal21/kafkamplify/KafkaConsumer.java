package com.github.rahulpal21.kafkamplify;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

@Slf4j
@Service
public class KafkaConsumer {

    @Autowired
    private ExecutorService threadPoolExecutor;
    private Map<String, KafkamplifyTask<String, String>> taskContainer = new ConcurrentHashMap<>();
    private KafkamplifyKeyExtractor keyExtractor;
    private final LongCounter batchCount = GlobalOpenTelemetry.get().meterBuilder("").build().counterBuilder("batchCount").build();
    private final LongHistogram batchLatency = GlobalOpenTelemetry.get().meterBuilder("").build().histogramBuilder("batchCount").ofLongs().build();

    @KafkaListener(id = "defaultlistener",
            topics = {"test-topic"}, batch = "true")
    public void listen(List<ConsumerRecord<String, String>> records) {
        long before = System.nanoTime();

        batchCount.add(records.size());

        List<KafkamplifyTask> tasks = new ArrayList<>();
        records.forEach(record -> {
            tasks.add(submitTask(record));
        });

        log.trace("******************** ALL SUBMITTED ******************");
        tasks.forEach(stringForkJoinTask -> {
            try {
                stringForkJoinTask.getTaskFuture().get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
            batchLatency.record(System.nanoTime() - before);
        });
        log.trace("******************* ALL PROCESSED **********************");
    }

    private KafkamplifyTask submitTask(ConsumerRecord<String, String> record) {
        String key = keyExtractor != null ? keyExtractor.extractKey(record) : UUID.randomUUID().toString();

        KafkamplifyTask task = taskContainer.get(key);

        if (task == null) {
            return initializeAndSubmit(record, key);
        } else {
            try {
                task.enqueue(record);
                return task;
            } catch (QueueClosedException e) {
                return initializeAndSubmit(record, key);
            }
        }
    }

    private KafkamplifyTask initializeAndSubmit(ConsumerRecord<String, String> record, String key) {
        KafkamplifyTask task = new KafkamplifyTask(record);
        taskContainer.put(key, task);
        task.setTaskFuture(threadPoolExecutor.submit(task));
        return task;
    }
}
