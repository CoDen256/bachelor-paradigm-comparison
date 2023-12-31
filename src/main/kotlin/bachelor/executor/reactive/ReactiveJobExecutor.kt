package bachelor.executor.reactive

import bachelor.core.api.JobApi
import bachelor.core.api.ResourceEvent
import bachelor.core.api.isPodRunningOrTerminated
import bachelor.core.api.isPodTerminated
import bachelor.core.api.snapshot.*
import bachelor.core.executor.*
import org.apache.logging.log4j.LogManager
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.kotlin.core.publisher.toMono
import java.time.Duration

class ReactiveJobExecutor(val api: JobApi) : JobExecutor {

    private val logger = LogManager.getLogger()


    override fun execute(request: JobExecutionRequest): ExecutionSnapshot {
        request.javaClass
        return executeUntilTerminated(request).toFuture() ?: ExecutionSnapshot(
            Logs.empty(),
            InitialJobSnapshot,
            InitialPodSnapshot
        )
    }

    fun executeUntilTerminated(request: JobExecutionRequest): Mono<ExecutionSnapshot> {
        val jobEventSink = Sinks.many().multicast().onBackpressureBuffer<ResourceEvent<ActiveJobSnapshot>>()
        val podEventSink = Sinks.many().multicast().onBackpressureBuffer<ResourceEvent<ActivePodSnapshot>>()

        val podListener = ResourceEventSinkAdapter(podEventSink)
        val jobListener = ResourceEventSinkAdapter(jobEventSink)

        val jobSnapshotStream = jobEventSink.asFlux().cache().flatMap { it.element.toMono() }
        val podSnapshotStream = podEventSink.asFlux().cache().flatMap { it.element.toMono() }


        // deserialize job spec, create and run the job in the cluster
        return Mono.fromCallable {
            api.addJobEventHandler(jobListener)
            api.addPodEventHandler(podListener)
            api.create(request.jobSpec)
        }.flatMap { job ->
            mono(podSnapshotStream, jobSnapshotStream, job, request)
                .doOnNext { api.delete(job) }
                .doOnError { api.delete(job) }
                .doOnCancel { api.delete(job) }
        }.doFinally {
                jobEventSink.tryEmitComplete()
                podEventSink.tryEmitComplete()
                api.removeJobEventHandler(jobListener)
                api.removePodEventHandler(podListener)
        }
    }

    private fun mono(
        podSnapshotStream: Flux<ActivePodSnapshot>,
        jobSnapshotStream: Flux<ActiveJobSnapshot>,
        job: JobReference,
        request: JobExecutionRequest
    ): Mono<ExecutionSnapshot> {
        // filter relevant snapshots and combine both streams, providing initial snapshots. cache(1) so .next() provides the latest available snapshot
        val stream = filterAndCombineSnapshots(podSnapshotStream, jobSnapshotStream, job.uid)
            .transform { logAsTimed(it) }
            .cache(1)

        // get next snapshot, where pod is terminated
        return nextTerminatedSnapshot(stream, request.isRunningTimeout, request.isTerminatedTimeout)
    }

    internal fun filterAndCombineSnapshots(
        pods: Flux<ActivePodSnapshot>,
        jobs: Flux<ActiveJobSnapshot>,
        uid: String
    ): Flux<ExecutionSnapshot> {

        val relevantJobStream = jobs.filter { it.uid == uid }
        val relevantPodStream = pods.filter { it.controllerUid == uid }

        val relevantJobStreamWithInitialSnapshot = insertAtStart(InitialJobSnapshot, relevantJobStream)
        val relevantPodStreamWithInitialSnapshot = insertAtStart(InitialPodSnapshot, relevantPodStream)
        return Flux.combineLatest(relevantJobStreamWithInitialSnapshot, relevantPodStreamWithInitialSnapshot) { j, p ->
            ExecutionSnapshot(Logs.empty(), j, p)
        }
    }

    private fun <T : Any> insertAtStart(element: T, source: Flux<out T>): Flux<T> {
        return Flux.concat(Mono.just(element), source)
    }


    internal fun nextTerminatedSnapshot(
        stream: Flux<ExecutionSnapshot>,
        isRunningTimeout: Duration,
        isTerminatedTimeout: Duration
    ): Mono<ExecutionSnapshot> {
        return stream
            // Wait for running or terminated pod. On timeout, get the latest snapshot with logs and convert to an exception
            .filter { isPodRunningOrTerminated(it.podSnapshot) }
            .timeoutFirst(isRunningTimeout, latestSnapshotWithLogs(stream).flatMap { PodNotRunningTimeoutException(it, isRunningTimeout).toMono() })
            // Wait for terminated pod. On timeout, get the latest snapshot with logs and convert to an exception
            .filter { isPodTerminated(it.podSnapshot) }
            .timeoutFirst(isTerminatedTimeout, latestSnapshotWithLogs(stream).flatMap { PodNotTerminatedTimeoutException(it, isTerminatedTimeout).toMono() })
            // Take the first from the latest available snapshots, that contains terminated pod
            .next()
            .flatMap { populateWithLogs(it) }
            .flatMap { verifyTermination(it) }
    }


    private fun <T> Flux<T>.timeoutFirst(timeout: Duration, fallback: Publisher<T>): Flux<T> {
        val firstItemTimeout = Mono.delay(timeout)
        val subsequentItemsTimeout = Mono.never<Any>()
        return timeout(firstItemTimeout, { subsequentItemsTimeout }, fallback)
    }


    private fun verifyTermination(snap: ExecutionSnapshot): Mono<ExecutionSnapshot> {
        // isPodTerminated is true at this point
        val terminatedState = (snap.podSnapshot as ActivePodSnapshot).mainContainerState as TerminatedState
        if (terminatedState.exitCode != 0) {
            return PodTerminatedWithErrorException(snap, terminatedState.exitCode).toMono()
        }
        return Mono.just(snap)
    }


    private fun latestSnapshotWithLogs(stream: Flux<ExecutionSnapshot>): Mono<ExecutionSnapshot> {
        return Mono.defer { stream.next().flatMap { populateWithLogs(it) } }
    }


    private fun populateWithLogs(snapshot: ExecutionSnapshot): Mono<ExecutionSnapshot> {
        return getLogs(snapshot.podSnapshot).map { ExecutionSnapshot(it, snapshot.jobSnapshot, snapshot.podSnapshot) }
            .onErrorComplete()
            .switchIfEmpty(snapshot.toMono())
    }

    private fun getLogs(podSnapshot: PodSnapshot): Mono<Logs> {
        if (podSnapshot !is ActivePodSnapshot) return Mono.empty()
        return api.getLogs(podSnapshot.reference()).toMono().map { Logs(it) }
    }


    private fun logAsTimed(stream: Flux<ExecutionSnapshot>): Flux<ExecutionSnapshot> {
        return if (!logger.isDebugEnabled) stream else stream.timed()
            .doOnNext {
                logger.info(
                    "(E): ${
                        it.elapsedSinceSubscription().toMillis()
                    }ms - ${it.get()}. ${System.currentTimeMillis()}"
                )
            }
            .map { it.get() }
    }

}