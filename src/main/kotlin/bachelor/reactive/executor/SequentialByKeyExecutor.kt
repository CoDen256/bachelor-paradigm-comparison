package calculations.runner.executor

import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.time.Duration

/**
 * The [SequentialByKeyExecutor] accepts a task and places it in a queue of
 * the tasks sharing the same key. Thus, the tasks for the same key will be
 * executed sequentially one after the other. Tasks for different keys will
 * be placed in different queues. Tasks are chained by invoking [Mono.then]
 * calls, so each task waits for completion of all previous ones within one
 * queue. The results are cached, so new subscriptions will not trigger new
 * execution of the task
 *
 * @property executionTimeout specifies a deadline for the task to
 *     complete. If the task exceeds the deadline, it will generate a
 *     timeout exception for its subscribers, but it will propagate the
 *     completion signal for the next tasks. So the next tasks will
 *     continue executing after the previous one
 */
class SequentialByKeyExecutor<T>(private val executionTimeout: Duration) {

    /** The latest task in the queue by key (can be considered as a queue) */
    private val previousTaskByKey = HashMap<String, Mono<T>>()

    /**
     * Submit the given task supplier as a deferred task.
     *
     * @param key the key to enqueue the task by
     * @param task the task supplier
     */
    fun deferAndSubmit(key: String, task: () -> Mono<T>): Mono<T> {
        return submit(key, Mono.defer(task))
    }

    /**
     * Submit the deferred task for execution in a queue determined by the key.
     * The tasks in the same queue will be executed sequentially.
     *
     * If the queue for the given key is empty, [Mono.empty] is used as the
     * initial task.
     *
     * The task is then placed in the queue by chaining [Mono.then] calls. The
     * task will not be executed, until the previous tasks are completed. Each
     * task will generate a timeout exception for its direct subscriber if no
     * value is emitted within the specified [executionTimeout] and propagate
     * a completion signal for the next tasks. So the next task will continue
     * execution after the previous task either completes with an error or a
     * value. The results of the tasks are cached, so new subscription will not
     * trigger new execution of the tasks.
     *
     * @param key the key to determine to which queue the task should be
     *     submitted.
     * @param deferredTask the actual task. The actual execution should start
     *     only upon subscription
     * @return the task, that, upon subscription, will wait for all previous
     *     tasks to complete
     */
    fun submit(key: String, deferredTask: Mono<T>): Mono<T> {
        previousTaskByKey.computeIfAbsent(key) { Mono.empty() }
        return previousTaskByKey.computeIfPresent(key) { _, prev ->
            waitPreviousThenExecuteNext(key, prev, deferredTask)
        }!!
    }

    /**
     * Waits for the given [previous] task to complete and executes the
     * [nextDeferred] right after. The execution of [nextDeferred] should be
     * performed within a specified deadline ([executionTimeout]) or a timeout
     * exception is generated for its subscribers. If the previous task is
     * completed with an error, it will be mapped to a completion signal for
     * the subsequent tasks. The [nextDeferred] will be executed directly after
     * the [previous] finishes execution or results in error. The result of the
     * [nextDeferred] execution is then cached, so the subscriptions from the
     * subsequent tasks will not trigger a new execution of the task
     *
     * @param key the key of the queue
     * @param previous the previous task to wait for
     * @param nextDeferred the next task to execute after the [previous]
     * @return the task, that waits [previous] and executes [nextDeferred]
     *     right after.
     */
    private fun waitPreviousThenExecuteNext(key: String, previous: Mono<T>, nextDeferred: Mono<T>): Mono<T> {
        val next = nextDeferred
            .timeout(executionTimeout, TaskTimeoutException(key, executionTimeout).toMono())
        return previous
            .onErrorComplete() // ignore error of the previous task and use it as completion signal
            .then(next)        // wait for the completion of the previous and create a task with a timeout
            .cache()           // cache the result, so the next task on the chain won't invoke the execution of this (previous) task
    }

    class TaskTimeoutException(key: String, timeout: Duration): Exception("Task for $key did not complete within specified deadline: ${timeout.toSeconds()} s")
}