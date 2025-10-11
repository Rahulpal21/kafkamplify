package com.github.rahulpal21.kafkamplify;

public class QueueClosedException extends Exception {
    public QueueClosedException(String message) {
        super(message);
    }

    public QueueClosedException() {

    }
}
