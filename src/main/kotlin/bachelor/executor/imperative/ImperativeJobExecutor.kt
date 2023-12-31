package bachelor.executor.imperative

import bachelor.core.api.JobApi
import bachelor.core.api.ResourceEvent
import bachelor.core.api.snapshot.*
import bachelor.core.currentTime
import bachelor.core.executor.*
import java.time.Duration
import java.util.concurrent.*
import java.util.function.Predicate

class ImperativeJobExecutor(private val api: JobApi) : JobExecutor {

    override fun execute(request: JobExecutionRequest): ExecutionSnapshot {
        val cachedJobEvents = ConcurrentLinkedQueue<ResourceEvent<ActiveJobSnapshot>>() // should concurrent be here?
        val cachedPodEvents = ConcurrentLinkedQueue<ResourceEvent<ActivePodSnapshot>>()
        var job: JobReference? = null
        val jobListener = ResourceEventCollectionAdapter(cachedJobEvents)
        val podListener = ResourceEventCollectionAdapter(cachedPodEvents)
        try {
            api.addJobEventHandler(jobListener)
            api.addPodEventHandler(podListener)
            println("---------------------[HANDLER ADDED ${currentTime()}]------------------------------")
            job = api.create(request.jobSpec)
            println("---------------------[CREATED JOB ${currentTime()}]------------------------------")

            val checkRunningCondition = checkPodCondition(
                request.isRunningTimeout,
                "IS RUNNING",
                cachedPodEvents
            ) { list -> list.any { it.mainContainerState is RunningState || it.mainContainerState is TerminatedState } }

            val checkTerminatedCondition = checkPodCondition(
                request.isTerminatedTimeout,
                "IS TERMINATED",
                cachedPodEvents
            ) { list -> list.any { it.mainContainerState is TerminatedState } }

            waitUntilDone("[WAITER IS RUN] ", request.isRunningTimeout, checkRunningCondition)
            val (pod: PodSnapshot?, currentState) = lastEvent(cachedJobEvents, job, cachedPodEvents)

            if (!isRunning(pod) && !isTerminated(pod)) {
                throw PodNotRunningTimeoutException(populateWithLogs(currentState), request.isRunningTimeout)
            }

            if (isTerminated(pod)) {
                return verifyExitCode((pod as ActivePodSnapshot).mainContainerState as TerminatedState, currentState)
            }

            waitUntilDone("[WAITER IS TERM] ", request.isTerminatedTimeout - request.isRunningTimeout, checkTerminatedCondition)
            val (newPodSnapshot: PodSnapshot?, newState) = lastEvent(cachedJobEvents, job, cachedPodEvents)

            if (!isTerminated(newPodSnapshot)) {
                throw PodNotTerminatedTimeoutException(populateWithLogs(newState), request.isTerminatedTimeout)
            }
            return verifyExitCode((newPodSnapshot as ActivePodSnapshot).mainContainerState as TerminatedState, newState)
        } finally {
            job?.let { api.delete(it) }
            api.removeJobEventHandler(jobListener)
            api.removePodEventHandler(podListener)
        }
    }

    private fun isRunning(podSnapshot: PodSnapshot?) =
        podSnapshot is ActivePodSnapshot && (podSnapshot.mainContainerState is RunningState)

    private fun isTerminated(newPodSnapshot: PodSnapshot?) =
        newPodSnapshot is ActivePodSnapshot && newPodSnapshot.mainContainerState is TerminatedState

    private fun verifyExitCode(mainContainerState: TerminatedState, currentState: ExecutionSnapshot): ExecutionSnapshot {
        return if (mainContainerState.exitCode == 0) {
            populateWithLogs(currentState)
        } else {
            throw PodTerminatedWithErrorException(populateWithLogs(currentState), mainContainerState.exitCode)
        }
    }

    private fun lastEvent(
        cachedJobEvents: ConcurrentLinkedQueue<ResourceEvent<ActiveJobSnapshot>>,
        job: JobReference,
        cachedPodEvents: ConcurrentLinkedQueue<ResourceEvent<ActivePodSnapshot>>
    ): Pair<PodSnapshot?, ExecutionSnapshot> {
        val jobSnapshot: JobSnapshot? = cachedJobEvents.findLast { it.element?.uid == job.uid }?.element
        val podSnapshot: PodSnapshot? = cachedPodEvents.findLast { it.element?.controllerUid == job.uid }?.element
        val currentState = ExecutionSnapshot(
            Logs.empty(),
            jobSnapshot ?: InitialJobSnapshot,
            podSnapshot ?: InitialPodSnapshot
        )
        return Pair(podSnapshot, currentState)
    }

    private fun populateWithLogs(s: ExecutionSnapshot): ExecutionSnapshot {
        return ExecutionSnapshot(getLogs(s.podSnapshot), s.jobSnapshot, s.podSnapshot)
    }

    private fun getLogs(podSnapshot: PodSnapshot?): Logs {
        val logs = (podSnapshot as? ActivePodSnapshot)?.let {
            try {
                api.getLogs(it.reference())
            } catch (e: Exception) {
                null
            }
        }
        return Logs(logs)
    }


    private fun <T> waitUntilDone(name: String, timeout: Duration, future: Future<T>): T? {
        return try {
            println("$name started / ${currentTime()}")
            val r = future.get()
            println("$name success $r / ${currentTime()}")
            return r
        } catch (e: TimeoutException) {
            println("$name fail $e / ${currentTime()}")
            null
        } catch (e: ExecutionException) {
            println("$name fail $e / ${currentTime()}")
            null
        } catch (e: Exception) {
            println("$name fail $e / ${currentTime()}")
            null
        } finally {
            println("$name cancelling [c: ${future.isCancelled} d:${future.isDone}] / ${currentTime()}")
            future.cancel(true)
            println("$name ended [c: ${future.isCancelled} d:${future.isDone}] / ${currentTime()}")
        }
    }


    private fun checkPodCondition(
        timeout: Duration,
        name: String,
        events: Collection<ResourceEvent<ActivePodSnapshot>>,
        condition: Predicate<List<ActivePodSnapshot>>
    ): Future<List<ActivePodSnapshot?>> {
        val future = CompletableFuture<List<ActivePodSnapshot?>>()
        future.completeAsync {
            println("[$name] START / ${currentTime()}")
            var i = 0
            while (true) {
                println("[$name] CHECK $i [c: ${future.isCancelled} e: ${future.isCompletedExceptionally} d:${future.isDone}] + ${events.mapNotNull { it.element }} / ${currentTime()}")
                if (condition.test(events.mapNotNull { it.element })
                    || future.isCancelled
                    || future.isCompletedExceptionally
                    || future.isDone
                ) {
                    println("[$name] FULFILLED [c: ${future.isCancelled} e: ${future.isCompletedExceptionally} d:${future.isDone}] + ${events.mapNotNull { it.element }}")
                    break
                }
                println("[$name] SLEEP ${i++}, ${events.mapNotNull { it.element }}")
                Thread.sleep(10)
            }
            if (future.isCancelled) {
                println("[$name] CANCEL")
            } else if (future.isCompletedExceptionally) {
                println("[$name] EXCEPTION")
            } else {
                println("[$name] DONE ${events.map { it.element }}")
            }
            println("[$name] END / ${currentTime()}")
            events.map { it.element }

        }
        return future.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS)
    }
}