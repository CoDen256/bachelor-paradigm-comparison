package bachelor.executor.imperative

import bachelor.core.api.*
import bachelor.core.api.snapshot.*
import bachelor.core.executor.*
import java.time.Duration
import java.util.concurrent.*
import java.util.function.Predicate

class ImperativeJobExecutor(private val api: JobApi): JobExecutor {

    override fun execute(request: JobExecutionRequest): ExecutionSnapshot {
        val cachedJobEvents = ConcurrentLinkedQueue<ResourceEvent<ActiveJobSnapshot>>() // should it be here?
        val cachedPodEvents = ConcurrentLinkedQueue<ResourceEvent<ActivePodSnapshot>>()
        var job: JobReference? = null
        val jobListener = ResourceEventHandler {
            cachedJobEvents.add(it)
        }
        val podListener = ResourceEventHandler {
            cachedPodEvents.add(it)
        }
        try {
            api.addJobEventHandler(jobListener)
            api.addPodEventHandler(podListener)
            job = api.create(request.jobSpec)


            waitUntilDone(request.isRunningTimeout, checkPodCondition(request.isRunningTimeout, "running${request.jobSpec}", cachedPodEvents, )
            {list -> list.any { it.mainContainerState is RunningState || it.mainContainerState is TerminatedState }
            })
            val future = checkPodCondition(
                request.isTerminatedTimeout - request.isRunningTimeout,
                "termin ${request.jobSpec}",
                cachedPodEvents
            ) { list -> list.any { it.mainContainerState is TerminatedState } }

            val (podSnapshot: PodSnapshot?, currentState) = lastEvent(cachedJobEvents, job, cachedPodEvents)


            if (podSnapshot is ActivePodSnapshot && (podSnapshot.mainContainerState is RunningState)){
                println("OOPSIE $podSnapshot")

                val done = waitUntilDone(request.isTerminatedTimeout - request.isRunningTimeout, future)
                val (newPodSnapshot: PodSnapshot?, newState) = lastEvent(cachedJobEvents, job, cachedPodEvents)

                if (newPodSnapshot is ActivePodSnapshot && newPodSnapshot.mainContainerState !is TerminatedState) {
                    throw PodNotTerminatedTimeoutException(populateWithLogs(newState), request.isTerminatedTimeout)
                }
                if (isTerminated(newPodSnapshot)) {
                    return verifyExitCode((newPodSnapshot as ActivePodSnapshot).mainContainerState as TerminatedState, newState)
                }
            } else if (isTerminated(podSnapshot)){
                return verifyExitCode((podSnapshot as ActivePodSnapshot).mainContainerState as TerminatedState, currentState)
            }
            throw PodNotRunningTimeoutException(populateWithLogs(currentState), request.isRunningTimeout)
        }finally {
            job?.let { api.delete(it) }
            api.removeJobEventHandler(jobListener)
            api.removePodEventHandler(podListener)
        }
    }

    private fun isTerminated(newPodSnapshot: PodSnapshot?) =
        newPodSnapshot is ActivePodSnapshot && newPodSnapshot.mainContainerState is TerminatedState

    private fun verifyExitCode(
        mainContainerState: TerminatedState,
        currentState: ExecutionSnapshot
    ): ExecutionSnapshot {
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
        return ExecutionSnapshot(Logs(getLogs(s.podSnapshot)), s.jobSnapshot, s.podSnapshot)
    }

    private fun getLogs(podSnapshot: PodSnapshot?): String? {
        val logs = (podSnapshot as? ActivePodSnapshot)?.let {
            try {
                api.getLogs(it.reference())
            } catch (e: Exception) {
                null
            }
        }
        return logs
    }


    private fun <T> waitUntilDone(timeout: Duration, future: Future<T>): T? {
        return try {
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: TimeoutException) {
            future.cancel(true)
            null
        } catch (e: ExecutionException) {
            future.cancel(true)
            null
        } catch (e: Exception) {
            future.cancel(true)
            null
        }
    }



    private fun checkPodCondition(timeout: Duration, name: String, events: Collection<ResourceEvent<ActivePodSnapshot>>, condition: Predicate<List<ActivePodSnapshot>>): Future<List<ActivePodSnapshot?>> {
        val future = CompletableFuture<List<ActivePodSnapshot?>>()
        future.completeAsync {
            var i = 0
            while (!condition.test(events.mapNotNull { it.element }) && !future.isCancelled && !future.isCompletedExceptionally) {
                println("Sleeping: $name ${i++}, ${events.mapNotNull { it.element }}")
                Thread.sleep(10)
            }
            if (future.isCancelled){
                println("Cancelled!! $name")
            }else if (future.isCompletedExceptionally) {
                println("EXCEPTIONALLY $name")
            }else{
                println("FINISHED $name ${events.map { it.element }}")
            }
            events.map { it.element }
        }
        return future.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS)

    // why not cancelling
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