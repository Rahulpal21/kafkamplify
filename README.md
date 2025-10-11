# kafkamplify
```mermaid
graph
    consumer --> batch --> TaskContainer --> ForkJoinPool
    ForkJoinPoolThreadFactory --> ForkJoinPoolWorkerThread --> ForkJoinPool
    WorkerThreadIdSemaphore --> ForkJoinPoolWorkerThread
    ForkJoinTask --> ForkJoinPool
%%    ForkJoinTask --> TaskContainer
    
```