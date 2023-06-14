package calculations.runner.executor

import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.CompletableFuture

class SequentialByKeyExecutor<T>(private val timeout: Duration) {

    private val previousTaskByKey = HashMap<String, Mono<T>>()

    fun executeAsMono(key: String, task: () -> CompletableFuture<T>): Mono<T> {
        previousTaskByKey.computeIfAbsent(key) { Mono.empty() }
        return previousTaskByKey.computeIfPresent(key) { _, prev -> prev
                .onErrorComplete()                            // ignore error of the previous task and use it as completion signal
                .then(Mono.fromFuture(task).timeout(timeout)) // wait for the completion of the previous and create a task with a timeout
                .cache()                                      // cache the result, so the next task on the chain won't invoke the execution of this (previous) task
        }!!
    }

}