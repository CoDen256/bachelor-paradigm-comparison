package bachelor.service.config.fabric8

import bachelor.reactive.kubernetes.ResourceEvent
import bachelor.service.api.ReactiveJobApi
import bachelor.service.api.resources.JobReference
import bachelor.service.api.resources.PodReference
import bachelor.service.api.snapshot.ActiveJobSnapshot
import bachelor.service.api.snapshot.ActivePodSnapshot
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
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Basic [ReactiveJobApi] implementation acting as a wrapper around the
 * [KubernetesClient] and providing methods to execute request in a
 * reactive manner.
 *
 * The [Fabric8ReactiveJobApi] uses internally to [Sinks.Many] sinks to
 * capture all the events produced by [SharedIndexInformer] both for pods
 * and jobs. The sinks will be later exposed to the client as [Flux]
 * allowing clients to subscribe to all events occurring in the given
 * [namespace]
 */
class Fabric8ReactiveJobApi(
    private val api: KubernetesClient,
    private val namespace: String
) : ReactiveJobApi {

    private val logger = LogManager.getLogger()

    private val jobEventSink = Sinks.many().multicast().onBackpressureBuffer<ResourceEvent<ActiveJobSnapshot>>()
    private val podEventSink = Sinks.many().multicast().onBackpressureBuffer<ResourceEvent<ActivePodSnapshot>>()
    private val cachedJobEvents = jobEventSink.asFlux().cache()
    private val cachedPodEvents = podEventSink.asFlux().cache()

    private var informersStarted = AtomicBoolean()
    private var jobInformer: SharedIndexInformer<Job>? = null
    private var podInformer: SharedIndexInformer<Pod>? = null

    override fun startListeners() {
        if (informersStarted.compareAndSet(false, true)){
            jobInformer = informOnJobEvents(jobEventSink)
            podInformer = informOnPodEvents(podEventSink)
        }else{
            error("Listeners are already started!")
        }
    }

    override fun create(spec: String): Mono<JobReference> {
        try {
            return api.batch().v1().jobs()
                .load(spec.byteInputStream(StandardCharsets.UTF_8))
                .create()
                .toMono()
                .map { JobReference(it.metadata.name, it.metadata.uid, it.metadata.namespace) }
                .map {
                    logger.info("Created {}", it)
                    it
                }
        } catch (e: IllegalArgumentException) {
            return InvalidJobSpecException("Unable to parse job spec: ${e.message}", e).toMono()
        } catch (e: KubernetesClientException) {
            if (e.code == 409)
                return JobAlreadyExistsException(
                    "Unable to create a new job, the job already exists: ${e.message}",
                    e
                ).toMono()
            return e.toMono()
        } catch (e: Exception) {
            return e.toMono()
        }
    }

    override fun delete(job: JobReference) {
        logger.info("Deleting job ${job.name}...")
        api.batch().v1().jobs()
            .inNamespace(job.namespace)
            .withName(job.name)
            .delete()
    }


    override fun podEvents(): Flux<ResourceEvent<ActivePodSnapshot>> {
        return cachedPodEvents
    }

    private fun informOnPodEvents(sink: Sinks.Many<ResourceEvent<ActivePodSnapshot>>): SharedIndexInformer<Pod> {
        return api.pods()
            .inNamespace(namespace)
            .inform(ResourceEventHandlerAdapter(sink) {
                it?.snapshot()
            })
    }

    override fun jobEvents(): Flux<ResourceEvent<ActiveJobSnapshot>> {
        return cachedJobEvents
    }

    private fun informOnJobEvents(sink: Sinks.Many<ResourceEvent<ActiveJobSnapshot>>): SharedIndexInformer<Job> {
        return api.batch()
            .v1()
            .jobs()
            .inNamespace(namespace)
            .inform(ResourceEventHandlerAdapter(sink) {
                it?.snapshot()
            })
    }

    override fun getLogs(pod: PodReference): Mono<String> {
        logger.info("Getting logs for ${pod.name}...")
        return try {
            api.pods()
                .inNamespace(pod.namespace)
                .withName(pod.name)
                .log.toMono()
        } catch (exception: Exception) {
            exception.toMono()
        }
    }

    override fun close() {
        logger.info("Closing Job API client and all the informers...")
        stopListeners()
        api.close()
    }

    override fun stopListeners() {
        logger.info("Stopping all the informers...")
        jobEventSink.tryEmitComplete()
        podEventSink.tryEmitComplete()
        jobInformer?.close()
        podInformer?.close()
    }
}