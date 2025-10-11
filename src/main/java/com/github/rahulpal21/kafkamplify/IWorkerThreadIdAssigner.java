package com.github.rahulpal21.kafkamplify;

public interface IWorkerThreadIdAssigner {
    AffinityPass assign(Thread worker) throws AffinityPassUnavailableException;

    void reclaim(AffinityPass pass);
}
