package com.github.rahulpal21.kafkamplify;

import com.github.rahulpal21.kafkamplify.api.KafkamplifyMessageHandler;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class KafkamplifyTask<K, V> implements Runnable {
    private final Queue<ConsumerRecord<K, V>> orderQueue;
    private final AtomicBoolean isClosed;
    @Autowired
    private KafkamplifyMessageHandler<K, V> handler;
    @Getter @Setter
    private Future<?> taskFuture;

    public KafkamplifyTask(ConsumerRecord<K, V> record) {
        orderQueue = new LinkedBlockingQueue<>();
        isClosed = new AtomicBoolean(false);
        orderQueue.add(record);
    }

    public void enqueue(ConsumerRecord<K, V> record) throws QueueClosedException {
        synchronized (orderQueue) {
            if (isClosed.get()) {
                throw new QueueClosedException();
            }
            orderQueue.add(record);
        }
    }

    @Override
    public void run() {
        do {
            ConsumerRecord<K, V> polled = null;
            synchronized (orderQueue) {
                polled = orderQueue.poll();
            }
            handler.handleMessage(polled);
        } while (checkQueue());

    }

    private boolean checkQueue() {
        synchronized (orderQueue) {
            boolean hasMore = orderQueue.peek() != null;
            if (!hasMore) {
                isClosed.set(true);
            }
            return hasMore;
        }
    }

}
