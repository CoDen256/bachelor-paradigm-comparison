package bachelor.core.api

import bachelor.executor.reactive.ResourceEvent
import bachelor.core.api.snapshot.JobReference
import bachelor.core.api.snapshot.PodReference
import bachelor.core.api.snapshot.ActiveJobSnapshot
import bachelor.core.api.snapshot.ActivePodSnapshot
import reactor.core.publisher.Flux

/**
 * [JobApi] is a simplified Kubernetes API client for managing
 * Kubernetes Jobs, Pods and observing events regarding Jobs and Pods within a namespace
 */
interface JobApi : AutoCloseable {

    /**
     * Starts the client and listening for the events. MUST be called only
     * once!
     *
     * @throws IllegalStateException if called more than once
     */
    fun startListeners()

    /**
     * Loads given job spec, creates a job from the spec and runs it on the
     * kubernetes cluster.
     *
     * @param spec the job spec
     * @return actual job created by the kubernetes controller, containing
     *     generated labels and metadata
     * @throws InvalidJobSpecException if the syntax of the spec is invalid
     * @throws JobAlreadyExistsException if the job with the same spec is
     *     already running
     */
    fun create(spec: String): JobReference

    /**
     * Removes the given job from the kubernetes cluster.
     *
     * @param job the job to delete
     */
    fun delete(job: JobReference)

    /**
     * Provides a [ResourceEvent] publisher, which internally via listeners
     * captures the events occurring in a particular namespace related to all
     * [Pod] resources in that namespace and publishes them as a stream. Each
     * event encapsulates an operation and a [Pod] instance (snapshot of the
     * pod at a given point of time), on which the operation was performed.
     * Thus, it is possible to observe any possible [Pod] event occurring in a
     * particular namespace. The underlying event stream should be cached, so
     * even if the subscribers subscribe after relevant events are emitted, the
     * events will still get processed
     *
     * @return a [Flux] publisher, which streams all the [Pod] events
     */
    fun podEvents(): List<ResourceEvent<ActivePodSnapshot>>

    /**
     * Provides a [ResourceEvent] publisher, which internally via listeners
     * captures the events occurring in a particular namespace related to all
     * [Job] resources in that namespace and publishes them as a stream. Each
     * event encapsulates an operation and a [Job] instance (snapshot of a
     * job at a given point of time), on which the operation was performed.
     * Thus, it is possible to observe any possible [Job] event occurring in
     * particular namespace. The underlying event stream should be cached, so
     * even if the subscribers subscribe after relevant events are emitted, the
     * events will still get processed
     *
     * @return a [Flux] publisher, which streams all the [Job] events
     */
    fun jobEvents(): List<ResourceEvent<ActiveJobSnapshot>>

    /**
     * Makes a request for the given pod to obtain the logs it has produced.
     *
     * @param pod the pod, which logs to request
     * @return the logs
     */
    fun getLogs(pod: PodReference): String

    /**
     * Stop the inner informers, that listen to the events regarding pods and
     * jobs on the namespace
     */
    fun stopListeners()
}