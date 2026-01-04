package com.github.rahulpal21.kafkamplify;

import com.github.rahulpal21.kafkamplify.api.KafkamplifyMessageHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Queue;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkamplifyRecursiveTask<K, V> extends RecursiveTask<String> {
    private final Queue<ConsumerRecord<K, V>> orderQueue;
    private final AtomicBoolean isClosed;
    private final ConsumerRecord<K, V> record;
    private KafkamplifyMessageHandler<K, V>  handler;

    public KafkamplifyRecursiveTask(ConsumerRecord<K, V> record) {
        orderQueue = new SynchronousQueue<>();
        isClosed = new AtomicBoolean(false);
        this.record = record;
    }

    private KafkamplifyRecursiveTask(ConsumerRecord<K, V> record, Queue<ConsumerRecord<K, V>> orderQueue, AtomicBoolean isClosed) {
        this.orderQueue = orderQueue;
        this.isClosed = isClosed;
        this.record = record;
    }

    public void enqueue(ConsumerRecord<K, V> record) throws QueueClosedException {
        synchronized (orderQueue) {
            if(isClosed.get()){
                throw new QueueClosedException();
            }
            orderQueue.add(record);
        }
    }

    @Override
    protected String compute() {
        //TODO task computation
        handler.handleMessage(record);
        //look for any enqueued tasks
        KafkamplifyRecursiveTask recursiveTask = null;

        do {
            ConsumerRecord<K, V> polled = null;
            synchronized (orderQueue){
                polled = orderQueue.poll();
            }
            handler.handleMessage(polled);
        }while(checkQueue());

    }

    private boolean checkQueue() {
        synchronized (orderQueue) {
            boolean hasMore = orderQueue.peek() != null;
            if(!hasMore){
                isClosed.set(true);
            }
            return hasMore;
        }
    }
}
