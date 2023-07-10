package bachelor.core.impl.api.fabric8

import bachelor.core.api.*
import bachelor.core.api.snapshot.*
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.batch.v1.Job
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientException
import io.fabric8.kubernetes.client.informers.SharedIndexInformer
import org.apache.logging.log4j.LogManager
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Basic [ReactiveJobApi] implementation acting as a wrapper around the
 * [KubernetesClient] and providing methods to execute request in a
 * reactive manner.
 *
 * The [Fabric8JobApi] uses internally to [Sinks.Many] sinks to capture all
 * the events produced by [SharedIndexInformer] both for pods and jobs. The
 * sinks will be later exposed to the client as [Flux] allowing clients to
 * subscribe to all events occurring in the given [namespace]
 */
class Fabric8JobApi(
    private val api: KubernetesClient,
    private val namespace: String
) : JobApi {

    private val logger = LogManager.getLogger()

    private var jobListeners = HashSet<ResourceEventHandler<ActiveJobSnapshot>>()
    private var podListeners = HashSet<ResourceEventHandler<ActivePodSnapshot>>()

    private var informersStarted = AtomicBoolean()
    private var jobInformer: SharedIndexInformer<Job>? = null
    private var podInformer: SharedIndexInformer<Pod>? = null

    override fun addPodEventHandler(listener: ResourceEventHandler<ActivePodSnapshot>) {
        podListeners.add(listener)
    }

    override fun addJobEventHandler(listener: ResourceEventHandler<ActiveJobSnapshot>) {
        jobListeners.add(listener)
    }

    override fun removePodEventHandler(listener: ResourceEventHandler<ActivePodSnapshot>) {
        podListeners.remove(listener)
    }

    override fun removeJobEventHandler(listener: ResourceEventHandler<ActiveJobSnapshot>) {
        jobListeners.remove(listener)
    }


    override fun startListeners() {
        if (informersStarted.compareAndSet(false, true)) {
            jobInformer = informOnJobEvents()
            podInformer = informOnPodEvents()
        } else {
            error("Listeners are already started!")
        }
    }

    override fun create(spec: String): JobReference {
        try {
            return api.batch().v1().jobs()
                .load(spec.byteInputStream(StandardCharsets.UTF_8))
                .create()
                .let { JobReference(it.metadata.name, it.metadata.uid, it.metadata.namespace) }
                .also {
                    logger.info("Created {}", it)
                }
        } catch (e: IllegalArgumentException) {
            throw InvalidJobSpecException("Unable to parse job spec: ${e.message}", e)
        } catch (e: KubernetesClientException) {
            if (e.code == 409)
                throw JobAlreadyExistsException(
                    "Unable to create a new job, the job already exists: ${e.message}",
                    e
                )
            throw e
        }
    }

    override fun delete(job: JobReference) {
        logger.info("Deleting job ${job.name}...")
        api.batch().v1().jobs()
            .inNamespace(job.namespace)
            .withName(job.name)
            .delete()
    }


    private fun informOnPodEvents(): SharedIndexInformer<Pod> {
        return api.pods()
            .inNamespace(namespace)
            .inform(ResourceEventHandlerAdapter(AggregatedEventHandler(podListeners)) {
                obj, action -> obj?.snapshot(action)
            })
    }

    private fun informOnJobEvents(): SharedIndexInformer<Job> {
        return api.batch()
            .v1()
            .jobs()
            .inNamespace(namespace)
            .inform(ResourceEventHandlerAdapter(AggregatedEventHandler(jobListeners)) {
                obj, action -> obj?.snapshot(action)
            })
    }

    override fun getLogs(pod: PodReference): String? {
        logger.info("Getting logs for ${pod.name}...")
        return api.pods()
            .inNamespace(pod.namespace)
            .withName(pod.name)
            .log
    }

    override fun close() {
        logger.info("Closing Job API client and all the informers...")
        stopListeners()
        api.close()
    }

    override fun stopListeners() {
        logger.info("Stopping all the informers...")
        jobInformer?.close()
        podInformer?.close()
    }
}