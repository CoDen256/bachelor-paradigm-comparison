package bachelor.core.impl.api.default

import bachelor.executor.reactive.Action
import bachelor.executor.reactive.ResourceEvent
import bachelor.core.api.JobApi
import bachelor.core.api.ResourceEventListener
import bachelor.core.api.snapshot.JobReference
import bachelor.core.api.snapshot.PodReference
import bachelor.core.api.snapshot.ActiveJobSnapshot
import bachelor.core.api.snapshot.ActivePodSnapshot
import bachelor.core.api.snapshot.Snapshot
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

    private var watches: MutableMap<ResourceEventListener<*>, Watch<*>> = HashMap()

    private val jobWatchResponseType = object : TypeToken<Watch.Response<V1Job>>() {}.type
    private val podWatchResponseType = object : TypeToken<Watch.Response<V1Pod>>() {}.type

    private val executor = Executors.newFixedThreadPool(2)

    override fun addPodListener(listener: ResourceEventListener<ActivePodSnapshot>) {
        val watch: Watch<V1Pod>
        Watch.createWatch<V1Pod>(client, watchPodCall, podWatchResponseType).also {
            watch = it
        }
        watches[listener] = watch
        executor.submit {
            while (watch.hasNext()){
                println("HASNEXT")
            }
            println("STOP")
            watch.forEachRemaining {
                listener.onEvent(mapWatchPodResponse(it))
            }
        }
    }

    override fun addJobListener(listener: ResourceEventListener<ActiveJobSnapshot>) {
        TODO("Not yet implemented")
    }

    override fun removeListener(listener: ResourceEventListener<out Snapshot>) {
        TODO("Not yet implemented")
    }
    // TODO: give from outside, maybe the same for fabric8

    override fun startListeners() {
        if (!watchesStarted.compareAndSet(false, true)) {
            error("Watches are already started!")
        }
//        val jobWatch: Watch<V1Job> = Watch.createWatch<V1Job?>(client, watchJobCall, jobWatchResponseType).also {
//            this.jobWatch = it
//        }
//        val podWatch: Watch<V1Pod> = Watch.createWatch<V1Pod?>(client, watchPodCall, podWatchResponseType).also {
//            this.podWatch = it
//        }
//        executor.submit {
//            jobWatch.forEachRemaining { cachedJobEvents.add(mapWatchJobResponse(it)) }
//        }
//        executor.submit {
//            podWatch.forEachRemaining { cachedPodEvents.add(mapWatchPodResponse(it)) }
//        }
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

    private fun mapWatchJobResponse(response: Watch.Response<V1Job>) =
        resourceEvent(response.type) { response.`object`.snapshot(it) }

    private fun mapWatchPodResponse(response: Watch.Response<V1Pod>) =
        resourceEvent(response.type) { response.`object`.snapshot(it) }

    override fun getLogs(pod: PodReference): String {
        return coreV1Api
            .readNamespacedPodLog(
                pod.name, pod.namespace, null, null, null, null, null, null, null, null, null
            )
    }

    override fun stopListeners() {
        watches.values.forEach { it.close() }
    }

    override fun close() {
        stopListeners()
    }

    private fun <T : Snapshot> resourceEvent(type: String, newObj: (Action) -> T): ResourceEvent<T> {
        val action = when (type) {
            "DELETED" -> Action.DELETE
            "ADDED" -> Action.ADD
            "MODIFIED" -> Action.UPDATE
            else -> Action.NOOP
        }
        return ResourceEvent(action, newObj(action))
    }
}