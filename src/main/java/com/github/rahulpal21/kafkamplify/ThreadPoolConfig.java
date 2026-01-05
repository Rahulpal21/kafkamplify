package com.github.rahulpal21.kafkamplify;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;

@Configuration
public class ThreadPoolConfig {

    @Bean
    public ExecutorService threadPoolExecutor() {
        int cores = Math.max(1, Runtime.getRuntime().availableProcessors());
        int corePoolSize = cores;
        int maxPoolSize = cores * 2;
        long keepAliveSeconds = 60L;
        int queueCapacity = 100;

        ThreadFactory threadFactory = Executors.defaultThreadFactory();

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveSeconds,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // Optionally allow core threads to time out
        executor.allowCoreThreadTimeOut(true);

        return executor;
    }
}
