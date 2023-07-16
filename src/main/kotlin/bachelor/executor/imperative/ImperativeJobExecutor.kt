package bachelor.executor.imperative

import bachelor.core.api.JobApi
import bachelor.core.api.ResourceEventHandler
import bachelor.core.api.isPodRunningOrTerminated
import bachelor.core.api.isPodTerminated
import bachelor.core.api.snapshot.*
import bachelor.core.executor.*
import java.time.Duration
import java.util.concurrent.*
import java.util.function.Predicate

class ImperativeJobExecutor(private val api: JobApi): JobExecutor {
    override fun execute(request: JobExecutionRequest): ExecutionSnapshot {
        val listener = ResourceEventHandler<ActiveJobSnapshot> { }
        val listener2 = ResourceEventHandler<ActivePodSnapshot> { }
        try {

            api.addJobEventHandler(listener)
            api.addPodEventHandler(listener2)
            TODO()

        }finally {
            api.removeJobEventHandler(listener)
            api.removePodEventHandler(listener2)
        }
    }

    private fun execute0(request: JobExecutionRequest): ExecutionSnapshot {
        var job: JobReference? = null
        val podEvents = ArrayList<PodSnapshot>().apply { add(InitialPodSnapshot) }
        val jobEvents = ArrayList<JobSnapshot>().apply { add(InitialJobSnapshot) }

        val podHandler = ResourceEventHandler<ActivePodSnapshot> { event -> event.element?.let { podEvents.add(it) } }
        val jobHandler = ResourceEventHandler<ActiveJobSnapshot> { event -> event.element?.let { jobEvents.add(it) } }
        try {
            api.addPodEventHandler(podHandler)
            api.addJobEventHandler(jobHandler)



            job = api.create(request.jobSpec)


            waitUntilPodRunningOrTerminated(request.isRunningTimeout)
            val pod = waitUntilPodTerminated(request.isTerminatedTimeout)

            verifyTermination(pod.mainContainerState as TerminatedState) // force cast because it was already checked

            return lastExecutionSnapshot(jobEvents, podEvents)
        } catch (e: ExceptionHolder) {
            throw e.supplyException(lastExecutionSnapshot(jobEvents, podEvents))
        } finally {
            job?.let { api.delete(it) }
            api.removePodEventHandler(podHandler)
            api.removeJobEventHandler(jobHandler)
        }
    }

    private fun lastExecutionSnapshot(
        jobEvents: ArrayList<JobSnapshot>,
        podEvents: ArrayList<PodSnapshot>,
    ): ExecutionSnapshot {
        val podReference = podEvents.lastOrNull()?.let {
            if (it is ActivePodSnapshot){
                it.reference()
            }else null
        }
        return ExecutionSnapshot(
            podReference?.let { Logs(api.getLogs(it)) } ?: Logs.empty(),
            jobEvents.last(),
            podEvents.last(),
        )
    }

    private fun waitUntilPodRunningOrTerminated(runningTimeout: Duration): ActivePodSnapshot {
        return waitUntilDone(runningTimeout, checkPodCondition { isPodRunningOrTerminated(it) })
            ?: throw ExceptionHolder{PodNotRunningTimeoutException(it, runningTimeout)}
    }

    private fun waitUntilPodTerminated(terminatedTimeout: Duration): ActivePodSnapshot {
        return waitUntilDone(terminatedTimeout, checkPodCondition { isPodTerminated(it) })
            ?: throw ExceptionHolder{PodNotTerminatedTimeoutException(it, terminatedTimeout)}
    }


    private fun <T> waitUntilDone(duration: Duration, future: Future<T>): T? {
        return try {
            if (duration.isNegative) {
                future.get()
            } else {
                future.get(duration.toNanos(), TimeUnit.NANOSECONDS)
            }
        } catch (e: TimeoutException) {
            null
        } catch (e: ExecutionException) {
            throw e
        } catch (e: Exception) {
            throw e
        }
    }

    private fun checkPodCondition(condition: Predicate<ActivePodSnapshot>): CompletableFuture<ActivePodSnapshot>{
        val future = CompletableFuture<ActivePodSnapshot>()

        val listener = ResourceEventHandler<ActivePodSnapshot>{
            try {
                if (it.element != null && condition.test(it.element)) {
                    future.complete(it.element)
                }
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }
        }
        api.addPodEventHandler(listener)

        return future.whenComplete { _, t ->
            api.removePodEventHandler(listener)
        }
    }


    private fun checkJobCondition(condition: Predicate<ActiveJobSnapshot?>): CompletableFuture<ActiveJobSnapshot>{
        val future = CompletableFuture<ActiveJobSnapshot>()

        val listener = ResourceEventHandler<ActiveJobSnapshot> {
            try {
                if (condition.test(it.element)) {
                    future.complete(it.element)
                }
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }
        }
        api.addJobEventHandler (listener)
        return future.whenComplete { _, t ->
            api.removeJobEventHandler(listener)
        }
    }

    private fun verifyTermination(state: TerminatedState) {
        if (state.exitCode != 0){
            throw ExceptionHolder{ PodTerminatedWithErrorException(it, state.exitCode)}
        }
    }

    class ExceptionHolder(val supplyException: (ExecutionSnapshot) -> Exception): Exception()

}