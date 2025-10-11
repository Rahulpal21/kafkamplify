package com.github.rahulpal21.kafkamplify;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

public class KafkamplifyThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
    private static final IWorkerThreadIdAssigner threadIdAssigner = new WorkerThreadIdAssigner();

    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        return new KafkamplifyWorkerThread(pool, threadIdAssigner);
    }

    class KafkamplifyWorkerThread extends ForkJoinWorkerThread {
        private final AffinityPass affinityPass;

        public KafkamplifyWorkerThread(ForkJoinPool pool, IWorkerThreadIdAssigner threadIdAssigner) {
            super(null, pool, false);
            affinityPass = threadIdAssigner.assign(this);
        }

        @Override
        protected void onStart() {
            super.onStart();
        }

        @Override
        protected void onTermination(Throwable errorIfAny) {
            super.onTermination(errorIfAny);
            threadIdAssigner.reclaim(affinityPass);
        }

    }

}

