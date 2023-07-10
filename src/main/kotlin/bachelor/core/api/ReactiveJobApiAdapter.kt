package bachelor.core.api

import bachelor.core.api.snapshot.ActiveJobSnapshot
import bachelor.core.api.snapshot.ActivePodSnapshot
import bachelor.core.api.snapshot.JobReference
import bachelor.core.api.snapshot.PodReference
import bachelor.executor.reactive.ResourceEvent
import org.apache.logging.log4j.LogManager
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.kotlin.core.publisher.toMono

/**
 * [AbstractReactiveJobApi] is a simplified Kubernetes API client for
 * managing Kubernetes Jobs, Pods and observing events regarding Jobs and
 * Pods within a namespace
 */
class ReactiveJobApiAdapter(
    private val api: JobApi
) : AutoCloseable, ReactiveJobApi {

    private val logger = LogManager.getLogger()

    private val jobEventSink = Sinks.many().multicast().onBackpressureBuffer<ResourceEvent<ActiveJobSnapshot>>()
    private val podEventSink = Sinks.many().multicast().onBackpressureBuffer<ResourceEvent<ActivePodSnapshot>>()
    private val cachedJobEvents = jobEventSink.asFlux().cache()
    private val cachedPodEvents = podEventSink.asFlux().cache()

    private val podListener = ResourceEventHandler { podEventSink.tryEmitNext(it) }
    private val jobListener = ResourceEventHandler { jobEventSink.tryEmitNext(it) }

    override fun startListeners() {
        api.addPodEventHandler(podListener)
        api.addJobEventHandler(jobListener)
        api.startListeners()
    }

    override fun create(spec: String): Mono<JobReference> {
        return try {
            api.create(spec).toMono()
        } catch (e: Exception) {
            e.toMono()
        }
    }

    override fun delete(job: JobReference) {
        api.delete(job)
    }

    override fun podEvents(): Flux<ResourceEvent<ActivePodSnapshot>> {
        return cachedPodEvents
    }

    override fun jobEvents(): Flux<ResourceEvent<ActiveJobSnapshot>> {
        return cachedJobEvents
    }

    override fun getLogs(pod: PodReference): Mono<String> {
        return try {
            api.getLogs(pod).toMono()
        } catch (e: Exception) {
            e.toMono()
        }
    }

    override fun stopListeners() {
        emitCompleteRemoveListeners()
        api.stopListeners()
    }

    override fun close() {
        emitCompleteRemoveListeners()
        api.close()
    }

    private fun emitCompleteRemoveListeners() {
        logger.info("Emitting Complete event")
        jobEventSink.tryEmitComplete()
        podEventSink.tryEmitComplete()
        logger.info("Removing listeners")
        api.removeJobEventHandler(jobListener)
        api.removePodEventHandler(podListener)

    }

}
