package bachelor.service.config.default

import bachelor.reactive.kubernetes.Action
import bachelor.reactive.kubernetes.ResourceEvent
import bachelor.service.api.JobApi
import bachelor.service.api.resources.JobReference
import bachelor.service.api.resources.PodReference
import bachelor.service.api.snapshot.ActiveJobSnapshot
import bachelor.service.api.snapshot.ActivePodSnapshot
import bachelor.service.api.snapshot.Snapshot
import com.google.gson.reflect.TypeToken
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.apis.BatchV1Api
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.util.Watch
import io.kubernetes.client.util.Yaml
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean


class DefaultKubernetesClientJobApi(
    private val client: ApiClient,
    private val namespace: String
) : JobApi {
    private val batchV1Api = BatchV1Api(client)
    private val coreV1Api = CoreV1Api(client)

    private val watchJobCall =
        batchV1Api.listNamespacedJobCall(namespace, null, null, null, null, null, null, null, null, null, true, null)
    private val watchPodCall =
        coreV1Api.listNamespacedPodCall(namespace, null, null, null, null, null, null, null, null, null, true, null)

    private val cachedJobEvents = ArrayList<ResourceEvent<ActiveJobSnapshot>>()
    private val cachedPodEvents = ArrayList<ResourceEvent<ActivePodSnapshot>>()

    private var watchesStarted = AtomicBoolean()

    private var jobWatch: Watch<V1Job>? = null
    private var podWatch: Watch<V1Pod>? = null
    private val jobWatchResponseType = object : TypeToken<Watch.Response<V1Job>>() {}.type
    private val podWatchResponseType = object : TypeToken<Watch.Response<V1Pod>>() {}.type

    private val executor = Executors.newFixedThreadPool(2)
    // TODO: give from outside, maybe the same for fabric8

    override fun startListeners() {
        if (!watchesStarted.compareAndSet(false, true)) {
            error("Watches are already started!")
        }
        val jobWatch: Watch<V1Job> = Watch.createWatch<V1Job?>(client, watchJobCall, jobWatchResponseType).also {
            this.jobWatch = it
        }
        val podWatch: Watch<V1Pod> = Watch.createWatch<V1Pod?>(client, watchPodCall, podWatchResponseType).also {
            this.podWatch = it
        }
        executor.submit {
            jobWatch.forEachRemaining { cachedJobEvents.add(mapWatchJobResponse(it)) }
        }
        executor.submit {
            podWatch.forEachRemaining { cachedPodEvents.add(mapWatchPodResponse(it)) }
        }
    }

    override fun create(spec: String): JobReference {
        val job = Yaml.load(spec) as V1Job
        return batchV1Api
            .createNamespacedJob(namespace, job, null, null, null, null)
            .let { JobReference(it.metadata!!.name!!, it.metadata!!.uid!!, it.metadata!!.namespace!!) }
    }

    override fun delete(job: JobReference) {
        batchV1Api // Propagation policy background deletes the dependents as well (the pod)
            .deleteNamespacedJob(job.name, namespace, null, null, null, null, "Background", null)
    }

    override fun podEvents(): List<ResourceEvent<ActivePodSnapshot>> {
        return cachedPodEvents
    }

    override fun jobEvents(): List<ResourceEvent<ActiveJobSnapshot>> {
        return cachedJobEvents
    }

    private fun mapWatchJobResponse(it: Watch.Response<V1Job>) = resourceEvent(it.type, it.`object`.snapshot())

    private fun mapWatchPodResponse(it: Watch.Response<V1Pod>) = resourceEvent(it.type, it.`object`.snapshot())

    override fun getLogs(pod: PodReference): String {
        return coreV1Api
            .readNamespacedPodLog(
                pod.name, pod.namespace, null, null, null, null, null, null, null, null, null
            )
    }

    override fun stopListeners() {
        jobWatch?.close()
        podWatch?.close()
    }

    override fun close() {
        stopListeners()
    }

    private fun <T : Snapshot> resourceEvent(type: String, obj: T): ResourceEvent<T> {
        return ResourceEvent(
            when (type) {
                "DELETED" -> Action.DELETE
                "ADDED" -> Action.ADD
                "MODIFIED" -> Action.UPDATE
                else -> Action.NOOP
            }, obj
        )
    }
}