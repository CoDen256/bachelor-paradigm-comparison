package bachelor.reactive.kubernetes.api

import bachelor.reactive.kubernetes.events.ResourceEvent
import bachelor.reactive.kubernetes.events.ResourceEventHandlerAdapter
import bachelor.service.executor.InvalidJobSpecException
import bachelor.service.executor.JobAlreadyExistsException
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.batch.v1.Job
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientException
import io.fabric8.kubernetes.client.informers.SharedIndexInformer
import org.apache.logging.log4j.LogManager
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.kotlin.core.publisher.toMono
import java.nio.charset.StandardCharsets

/**
 * Basic [JobApi] implementation acting as a wrapper around the
 * [KubernetesClient] and providing methods to execute request in a
 * reactive manner.
 *
 * The [BaseJobApi] uses internally to [Sinks.Many] sinks to capture all
 * the events produced by [SharedIndexInformer] both for pods and jobs. The
 * sinks will be later exposed to the client as [Flux] allowing clients to
 * subscribe to all events occurring in the given [namespace]
 */
class BaseJobApi(
    private val api: KubernetesClient,
    private val namespace: String
) : JobApi {

    private val logger = LogManager.getLogger()

    private val jobEventSink = Sinks.many().multicast().onBackpressureBuffer<ResourceEvent<Job>>()
    private val podEventSink = Sinks.many().multicast().onBackpressureBuffer<ResourceEvent<Pod>>()
    private val cachedJobEvents = jobEventSink.asFlux().cache()
    private val cachedPodEvents = podEventSink.asFlux().cache()

    private var jobInformer: SharedIndexInformer<Job>? = null
    private var podInformer: SharedIndexInformer<Pod>? = null

    override fun start() {
        jobInformer = informOnJobEvents(jobEventSink)
        podInformer = informOnPodEvents(podEventSink)
    }

    override fun create(spec: String): Mono<Job> {
        try {
            return api.batch().v1().jobs()
                .load(spec.byteInputStream(StandardCharsets.UTF_8))
                .create()
                .toMono()
        } catch (e: IllegalArgumentException) {
            return InvalidJobSpecException("Unable to parse job spec: ${e.message}", e).toMono()
        } catch (e: KubernetesClientException) {
            if (e.code == 409)
                return JobAlreadyExistsException("Unable to create a new job, the job already exists: ${e.message}", e).toMono()
            return e.toMono()
        } catch (e: Exception) {
            return e.toMono()
        }
    }

    override fun delete(job: Job) {
        logger.info("Deleting job ${job.metadata?.name}...")
        api.batch().v1().jobs().resource(job).delete()
    }

    override fun podEvents(): Flux<ResourceEvent<Pod>> {
        return cachedPodEvents
    }

    private fun informOnPodEvents(sink: Sinks.Many<ResourceEvent<Pod>>): SharedIndexInformer<Pod> {
        return api.pods()
            .inNamespace(namespace)
            .inform(ResourceEventHandlerAdapter(sink))
    }

    override fun jobEvents(): Flux<ResourceEvent<Job>> {
        return cachedJobEvents
    }

    private fun informOnJobEvents(sink: Sinks.Many<ResourceEvent<Job>>): SharedIndexInformer<Job> {
        return api.batch()
            .v1()
            .jobs()
            .inNamespace(namespace)
            .inform(ResourceEventHandlerAdapter(sink))
    }

    override fun getLogs(pod: Pod): Mono<String> {
        logger.info("Getting logs for ${pod.metadata?.name}...")
        return try {
            api.pods().resource(pod).log.toMono()
        } catch (exception: Exception) {
            exception.toMono()
        }
    }

    override fun close() {
        logger.info("Closing Job API client and all the informers...")
        jobEventSink.tryEmitComplete()
        podEventSink.tryEmitComplete()
        jobInformer?.close()
        podInformer?.close()
        api.close()
    }
}