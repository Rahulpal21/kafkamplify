package com.github.rahulpal21.kafkamplify;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

public class WorkerThreadIdAssigner implements IWorkerThreadIdAssigner {
    private static final int PERMIT_COUNT = 10;
    private final ConcurrentLinkedDeque<AffinityPass> stack;
    private final Map<AffinityPass, Thread> assignedPasses;
    
    public WorkerThreadIdAssigner() {
        initPool();
        stack = new ConcurrentLinkedDeque<>();
        assignedPasses = HashMap.newHashMap(PERMIT_COUNT);
    }

    private void initPool() {
        for (int i = 0; i < PERMIT_COUNT; i++) {
            stack.add(new AffinityPass(i));
        }
    }

    @Override
    public AffinityPass assign(Thread worker) throws AffinityPassUnavailableException{
        AffinityPass pass = stack.remove();
        if (pass == null){
            throw new AffinityPassUnavailableException();
        }
        assignedPasses.put(pass, worker);
        return pass;
    }

    @Override
    public void reclaim(AffinityPass pass) {
        assignedPasses.remove(pass);
        stack.add(pass);
    }
}
