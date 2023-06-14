package bachelor.reactive.kubernetes.executor

import bachelor.reactive.kubernetes.api.JobApi
import bachelor.reactive.kubernetes.api.PodNotRunningTimeoutException
import bachelor.reactive.kubernetes.api.PodNotTerminatedTimeoutException
import bachelor.reactive.kubernetes.api.PodTerminatedWithErrorException
import bachelor.reactive.kubernetes.api.snapshot.*
import calculations.runner.kubernetes.api.*
import bachelor.reactive.kubernetes.events.ResourceEvent
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.batch.v1.Job
import org.apache.logging.log4j.LogManager
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.time.Duration

/**
 * [KubernetesJobExecutor] executes [JobExecutionRequest] by loading
 * given job spec, creating it in the cluster and running it until its
 * termination.
 *
 * For given [JobExecutionRequest] the [KubernetesJobExecutor] executes it
 * as follows:
 * 1) Executes a request via [JobApi] to observe all events
 *    ([ResourceEvent]s) occurring in the given namespace both for jobs and
 *    pods. As a result, two [ResourceEventStream]s are created (for job
 *    events, as well as for pod events), which publish [ResourceEvent]s,
 *    captured internally by listeners.
 * 2) Each event in the streams is converted to a [JobSnapshot] or a
 *    [PodSnapshot], which represent a kubernetes resource state at a
 *    particular point in time.
 * 3) Executes a request to create a [Job] from given spec and run it
 *    inside the namespace of a cluster.
 * 4) The rest of the execution based on listening to the events occurring
 *    in the namespace regarding pods and jobs. The resulting event streams
 *    will later be transformed and filtered to wait and capture the latest
 *    desired state of a system. In case of [nextTerminatedSnapshot], the
 *    expected state is a snapshot of a system, in which the pod created by
 *    the job has reached its TERMINATED state.
 * 5) After the job has been created, two streams are filtered to contain
 *    only the events matching the job's UID, so it contains only the
 *    events relevant to this execution
 * 6) The streams are modified, so that as first elements they contain
 *    initial snapshots, that represent, that the pod or the job have not
 *    been yet created ([InitialPodSnapshot] and [InitialJobSnapshot]).
 *    Thus producing an event stream, that emits at least one value.
 * 7) Then both streams are combined so that each new event(or snapshot)
 *    emitted by any of the stream is combined with the latest value from
 *    the other stream. The resulting snapshot is an [ExecutionSnapshot]
 *    combining [PodSnapshot] and [JobSnapshot].
 * 8) The stream is cached(via [Flux.cache(1)]), to capture only the latest
 *    produced value. Any [Flux.next()] will return the latest available
 *    snapshot of the system.
 * 8) The resulting stream is then filtered to wait for the
 *    [ExecutionSnapshot] that corresponds to RUNNING(or TERMINATED) state
 *    of the pod. If the value is not emitted within the specified deadline
 *    (isRunningTimeout) the corresponding exception is thrown, containing
 *    the latest available snapshot of the system. It provides additional
 *    information about why the pod has not been run within the specified
 *    deadline.
 * 9) The same applies for isTerminatedTimeout. Which specifies, how much
 *    to wait until the system in the TERMINATED state. Once exceeded, the
 *    corresponding exception with the latest available snapshot is thrown.
 * 10) If the event stream publishes an event with the TERMINATED state and
 *     the timeouts weren't exceeded, the state will be verified, whether
 *     the pod has terminated successfully based on the exit code of the
 *     container. If it is a non-zero exit code, the exception is thrown.
 * 11) If the stream generates an event containing the successfully
 *     terminated pod, it will return the corresponding [ExecutionSnapshot]
 *     and populate it with logs, making the corresponding request to the
 *     [JobApi]
 * 12) If [executeAndReadLogs] is called, and the resulting [Mono] is
 *     successful, then the logs are returned back to the caller
 *
 * @property api the underlying kubernetes [JobApi] that is used to execute
 *     requests regarding kubernetes jobs
 */
// An example, how the original two streams are transformed and filtered.
// Pod- And JobSnapshot format: {JOB_UID}({STATE})
// ExecutionSnapshot format: {JOB_UID}({JOB_STATE}+{POD_STATE}+[{LOGS}])
// Job States are: Act(Active), Rdy(Ready), Fail(Failed), Succ(Succeeded)
// Pod States are: Wait(Waiting), Pend(Pending), Run(Running), Term(Terminated)
// InitJ refers to InitialJobSnapshot and InitP refers to InitialPodSnapshot
//
//         +----------------------------------------------------------------------------------+
//         | Observe job and pod event streams for namespace + convert events to snapshots    |
//         +----------------------------------------------------------------------------------+
//                                               |
//                                               |
//                                               V
// jobs: -----------------0(Act)-----1(Rdy)--------2(Fail)------0(Rdy)----------------0(Succ)-------------|>
// pods: ---3(Wait)--------------------0(Pend)-----0(Run)----1(Term)---------0(Term)-2(Term)--------------|>
//                                               |
//                                               |
//                                               V
//         +----------------------------------------------------------------------------------+
//         |        Filter jobs and pods with (uid=0) and insert initial snapshots            |
//         +----------------------------------------------------------------------------------+
//                                               |
//                                               |
//                                               V
// jobs: -(InitJ)---------0(Act)--------------------------------0(Rdy)----------------0(Succ)-------------|>
// pods: -(InitP)----------------------0(Pend)------0(Run)-----------------0(Term)------------------------|>
//                                               |
//                                               |
//                                               V
//       +------------------------------------------------------------------------------------+
//       |                   Combine both streams to ExecutionSnapshot                        |
//       +------------------------------------------------------------------------------------+
//                                               |
//                                               |
//                                               V
// stream:-(InitJ+InitP)--0(Act+InitP)--0(Act+Pend)--0(Act+Run)--0(Rdy+Run)--0(Rdy+Term)--0(Succ+Term)-----|>
//                                               |
//                                               |
//                                               V
//       +------------------------------------------------------------------------------------+
//       |        Filter snapshots, where pod is in state RUNNING OR TERMINATED               |
//       +------------------------------------------------------------------------------------+
//                                               |
//                                               |
//                                               V
// run:  +--------------------------------------------0(Act+Run)---0(Rdy+Run)--0(Rdy+Term)--0(Succ+Term)-----|>
//       |                                       |    ^          ^
//       |                                       |    |          |  if timeSinceSubscription > isRunningTimeout
//       +----------<timeSinceSubscription>------+----+          |  generate a PodNotRunningTimeoutException
//       +-----------<isRunningTimeout>----------+---------------+  with latest available snapshot (stream.cache(1).next())
//                                               |
//                                               V
//       +------------------------------------------------------------------------------------+
//       |                   Filter snapshots, where pod is in state TERMINATED               |
//       +------------------------------------------------------------------------------------+
//                                               |
//                                               |
//                                               |
//                                               V
// term: +---------------------------------------------------0(Rdy+Term)--0(Succ+Term)-----------------------|>
//       |                                       |           ^          ^
//       |                                       |           |          |   if timeSinceSubscription > isTerminatedTimeout
//       |----------<timeSinceSubscription>------+-----------+          |   generate a PodNotTerminatedTimeoutException
//       |-----------<isTerminatedTimeout>-------+----------------------+   with latest available snapshot (stream.cache(1).next())
//                                               |
//                                               V
//       +------------------------------------------------------------------------------------+
//       |                   Get next terminated snapshot and populate with Logs              |
//       +------------------------------------------------------------------------------------+
//                                               |
//                                               |
//                                               V
// result: ----------------------------------------------------0(Rdy + Term + [LOGS])----------------------|>
class KubernetesJobExecutor(val api: JobApi) {

    private val logger = LogManager.getLogger()

    /**
     * Execute the given [JobExecutionRequest] and wait until it produces
     * terminated [ExecutionSnapshot]. Read the logs of the [ExecutionSnapshot]
     * afterwards.
     *
     * @param request to execute a job
     * @return the logs, produced by the relating pod
     */
    fun executeAndReadLogs(request: JobExecutionRequest): Mono<String> {
        return executeUntilTerminated(request).map { it.logs.getContentOrEmpty() }
    }

    /**
     * Execute the given [JobExecutionRequest].
     * 1) Listen for events published by [ResourceEventStream] for given
     *    namespace, transform the events to [PodSnapshot] and [JobSnapshot]
     * 2) Create a job in the cluster based on [JobExecutionRequest] spec
     * 3) Filter the events from [ResourceEventStream] and combine
     *    [PodSnapshot] and [JobSnapshot] into [ExecutionSnapshot]
     * 3) Wait until [ResourceEventStream] produces an [ExecutionSnapshot]
     *    which corresponds to a pod in the TERMINATED state via
     *    [nextTerminatedSnapshot].
     * 4) When [nextTerminatedSnapshot] produces any value, error or is
     *    cancelled, clean up, by removing the job from the cluster
     *
     * @param request to execute
     * @return the next terminated [ExecutionSnapshot]
     */
    fun executeUntilTerminated(request: JobExecutionRequest): Mono<ExecutionSnapshot> {
        val jobSnapshotStream = api.jobEvents().flatMap { jobEventToSnapshot(it) }
        val podSnapshotStream = api.podEvents().flatMap { podEventToSnapshot(it) }

        // deserialize job spec, create and run the job in the cluster
        return api.create(request.jobSpec).flatMap { job ->
            // filter relevant snapshots and combine both streams, providing initial snapshots. cache(1) so .next() provides the latest available snapshot
            val stream = filterAndCombineSnapshots(podSnapshotStream, jobSnapshotStream, job.metadata.uid)
                .transform { logAsTimed(it) }
                .cache(1)

            // get next snapshot, where pod is terminated
            nextTerminatedSnapshot(stream, request.isRunningTimeout, request.isTerminatedTimeout)
                .doOnNext { api.delete(job) }
                .doOnError { api.delete(job) }
                .doOnCancel { api.delete(job) }
        }
    }

    /**
     * Filter and combine snapshots. To provide at least one snapshot in the
     * stream, representing an unavailable state, or that pod or job has
     * not been created, a [InitialJobSnapshot] and [InitialPodSnapshot]
     * inserted at the start of each stream. Filtering is performed based on
     * the matching of job's [uid] for both pod and jobs streams. Combining
     * [PodSnapshot] and [JobSnapshot] results in an [ExecutionSnapshot],
     * that contains the snapshot of the whole system, without any logs.
     *
     * @param pods the stream of [PodSnapshot]s
     * @param jobs the stream of [JobSnapshot]s
     * @param uid the uid to find the relevant pods and jobs
     * @return the combined stream containing [ExecutionSnapshot]
     */
    internal fun filterAndCombineSnapshots(
        pods: Flux<ActivePodSnapshot>,
        jobs: Flux<ActiveJobSnapshot>,
        uid: String
    ): Flux<ExecutionSnapshot> {

        val relevantJobStream = jobs.filter { it.job.metadata.uid == uid }
        val relevantPodStream = pods.filter { it.pod.metadata.labels["controller-uid"] == uid }

        val relevantJobStreamWithInitialSnapshot = insertAtStart(InitialJobSnapshot, relevantJobStream)
        val relevantPodStreamWithInitialSnapshot = insertAtStart(InitialPodSnapshot, relevantPodStream)
        return Flux.combineLatest(relevantJobStreamWithInitialSnapshot, relevantPodStreamWithInitialSnapshot) { j, p ->
            ExecutionSnapshot(Logs.empty(), j, p)
        }
    }

    /** Helper method to insert a value at the start of a [Flux] */
    private fun <T : Any> insertAtStart(element: T, source: Flux<out T>): Flux<T> {
        return Flux.concat(Mono.just(element), source)
    }


    /**
     * Wait and get next [ExecutionSnapshot] that corresponds to a pod in
     * TERMINATED state.
     * - The given stream is a hot publisher, emitting values independently of
     *   this subscription.
     * - Expects a stream, that always has at least one emitted value.
     * - Expects a stream, that caches the events and always provides the
     *   latest [ExecutionSnapshot] upon invoking next().
     * - If there are multiple terminated [ExecutionSnapshot]s emitted before
     *   subscription to the stream, then the last emitted-terminated snapshot
     *   will always be used, even if there are newer terminated snapshots,
     *   that will be emitted later.
     * - If there are terminated [ExecutionSnapshot]s emitted after
     *   subscription to the stream, then the first emitted snapshot will be
     *   used
     * - If there are no [ExecutionSnapshot]s with pod in RUNNING or TERMINATED
     *   state after [isRunningTimeout], a [PodNotRunningTimeoutException] is
     *   thrown.
     * - If there are no [ExecutionSnapshot]s with pod in TERMINATED state
     *   after [isTerminatedTimeout], a [PodNotTerminatedTimeoutException] is
     *   thrown
     * - If there is an [ExecutionSnapshot] with pod in TERMINATED state and
     *   the pod has exited with non-zero return code, a
     *   [PodTerminatedWithErrorException] is thrown
     * - Each exception contains the latest available snapshot from the stream,
     *   providing information of the current system state
     *
     * @param stream a hot publisher, containing [ExecutionSnapshot] of a
     *     system generated by the listeners of the Kubernetes namespace
     * @param isRunningTimeout defines, how much time is given for the pod to
     *     reach its RUNNING or TERMINATED state
     * @param isTerminatedTimeout defines, how much time is given for the pod
     *     to reach its TERMINATED state
     * @return latest available [ExecutionSnapshot] of a system with pod in
     *     TERMINATED state
     */
    internal fun nextTerminatedSnapshot(
        stream: Flux<ExecutionSnapshot>,
        isRunningTimeout: Duration,
        isTerminatedTimeout: Duration
    ): Mono<ExecutionSnapshot> {
        return stream
            // Wait for running or terminated pod. On timeout, get the latest snapshot with logs and convert to an exception
            .filter { isPodRunningOrTerminated(it) }
            .timeoutFirst(isRunningTimeout, latestSnapshotWithLogs(stream).flatMap { podNotRunningError(it, isRunningTimeout) })
            // Wait for terminated pod. On timeout, get the latest snapshot with logs and convert to an exception
            .filter { isPodTerminated(it) }
            .timeoutFirst(isTerminatedTimeout, latestSnapshotWithLogs(stream).flatMap { podNotTerminatedError(it, isTerminatedTimeout) })
            // Take the first from the latest available snapshots, that contains terminated pod
            .next()
            .flatMap { populateWithLogs(it) }
            .flatMap { verifyTermination(it) }
    }

    /**
     * Helper method to work around the [Flux.timeout] problem. It applies not
     * only for the first element, but for all subsequent elements, resulting
     * sometimes in a timeout, even if the first element arrived in time.
     *
     * @param timeout the actual timeout for the first item to arrive
     * @param fallback if first item is not emitted within specified timeout,
     *     use a fallback
     */
    private fun <T> Flux<T>.timeoutFirst(timeout: Duration, fallback: Publisher<T>): Flux<T> {
        val firstItemTimeout = Mono.delay(timeout)
        val subsequentItemsTimeout = Mono.never<Any>()
        return timeout(firstItemTimeout, { subsequentItemsTimeout }, fallback)
    }

    /**
     * Verify the given [ExecutionSnapshot] which corresponds to the pod in
     * state TERMINATED. The pod MUST BE terminated at this point. Verify that
     * the exit code of the pod is null. If not, then return an exception.
     */
    private fun verifyTermination(snap: ExecutionSnapshot): Mono<ExecutionSnapshot> {
        // isPodTerminated is true at this point
        val terminatedState = (snap.podSnapshot as ActivePodSnapshot).mainContainerState as TerminatedState
        if (terminatedState.exitCode != 0) {
            return podTerminatedWithErrorException(snap, terminatedState.exitCode)
        }
        return Mono.just(snap)
    }

    /**
     * Helper method to determine, whether the given [ExecutionSnapshot]
     * corresponds to a state, when the pod is in state TERMINATED
     */
    private fun isPodTerminated(e: ExecutionSnapshot): Boolean =
        e.podSnapshot is ActivePodSnapshot && e.podSnapshot.mainContainerState is TerminatedState

    /**
     * Helper method to determine, whether the given [ExecutionSnapshot]
     * corresponds to a state, when the pod is in state RUNNING or TERMINATED
     */
    private fun isPodRunningOrTerminated(e: ExecutionSnapshot): Boolean =
        e.podSnapshot is ActivePodSnapshot && (e.podSnapshot.mainContainerState is TerminatedState || e.podSnapshot.mainContainerState is RunningState)

    /**
     * Get the latest available snapshot from the stream and populate it with
     * logs. Returns a hot publisher. Thus, the actual .next() call will be
     * invoked only after the timeout and will provide the latest available
     * snapshot after the timeout. The [stream] must be a hot publisher, which
     * caches the latest value.
     *
     * @param stream - snapshot [Flux], caching last emitted element
     */
    private fun latestSnapshotWithLogs(stream: Flux<ExecutionSnapshot>): Mono<ExecutionSnapshot> {
        return Mono.defer { stream.next().flatMap { populateWithLogs(it) } }
    }

    /**
     * Helper method to populate given [ExecutionSnapshot] with logs produced
     * by the pod.
     */
    private fun populateWithLogs(snapshot: ExecutionSnapshot): Mono<ExecutionSnapshot> {
        return getLogs(snapshot.podSnapshot).map { ExecutionSnapshot(it, snapshot.jobSnapshot, snapshot.podSnapshot) }
//            .onErrorComplete()
            .switchIfEmpty(snapshot.toMono())
    }

    /**
     * Helper method to fetch the logs for the given [PodSnapshot]. If the pod
     * in the snapshot is unavailable (i.e. [InitialPodSnapshot]) then return
     * an empty [Mono]
     */
    private fun getLogs(podSnapshot: PodSnapshot): Mono<Logs> {
        if (podSnapshot !is ActivePodSnapshot) return Mono.empty()
        return api.getLogs(podSnapshot.pod).map { Logs(it) }
    }

    /**
     * Helper method to capture the [ExecutionSnapshot]s events emitted from
     * the stream and log them with additional measured time info.
     */
    private fun logAsTimed(stream: Flux<ExecutionSnapshot>): Flux<ExecutionSnapshot> {
        return if (!logger.isDebugEnabled) stream else stream.timed()
            .doOnNext { logger.info("(E): ${it.elapsedSinceSubscription().toMillis()}ms - ${it.get()}") }
            .map { it.get() }
    }

    private fun <T> podTerminatedWithErrorException(it: ExecutionSnapshot, exitCode: Int): Mono<T> {
        return PodTerminatedWithErrorException(it, exitCode).toMono()
    }

    private fun <T> podNotTerminatedError(snapshot: ExecutionSnapshot, timeout: Duration): Mono<T> {
        return PodNotTerminatedTimeoutException(snapshot, timeout).toMono()
    }

    private fun <T> podNotRunningError(snapshot: ExecutionSnapshot, timeout: Duration): Mono<T> {
        return PodNotRunningTimeoutException(snapshot, timeout).toMono()
    }

    /**
     * Create a snapshot of the job object contained in the event. If the event
     * has no resource object, return an empty [Mono]
     */
    private fun jobEventToSnapshot(jobEvent: ResourceEvent<Job>): Mono<ActiveJobSnapshot> {
        return jobEvent.element?.snapshot(jobEvent.action).toMono()
    }

    /**
     * Create a snapshot of the pod object contained in the event. If the event
     * has no resource object, return an empty [Mono]
     */
    private fun podEventToSnapshot(podEvent: ResourceEvent<Pod>): Mono<ActivePodSnapshot> {
        return podEvent.element?.snapshot(podEvent.action).toMono()
    }
}