package bachelor.core.impl.api.default

import bachelor.core.api.Action
import bachelor.core.api.ResourceEvent
import bachelor.core.api.JobApi
import bachelor.core.api.ResourceEventHandler
import bachelor.core.api.snapshot.*
import com.google.gson.reflect.TypeToken
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.apis.BatchV1Api
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.util.Watch
import io.kubernetes.client.util.Yaml
import java.lang.RuntimeException
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



    private var watchesStarted = AtomicBoolean()

    private var jobListeners = HashSet<ResourceEventHandler<ActiveJobSnapshot>>()
    private var podListeners = HashSet<ResourceEventHandler<ActivePodSnapshot>>()

    private var jobWatch: Watch<V1Job>? = null
    private var podWatch: Watch<V1Pod>? = null

    private val jobWatchResponseType = object : TypeToken<Watch.Response<V1Job>>() {}.type
    private val podWatchResponseType = object : TypeToken<Watch.Response<V1Pod>>() {}.type

    private val executor = Executors.newFixedThreadPool(2)

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

    // TODO: give executor from outside, maybe the same for fabric8
    // todo: maybe in init{}
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
            try {
                jobWatch.forEachRemaining { event ->
                    jobListeners.forEach {
                        it.onEvent(mapWatchJobResponse(event))
                    }
                }
            }catch (e: RuntimeException){
                println("Job watch ended")
            }

        }
        executor.submit {
            try {
                podWatch.forEachRemaining { event ->
                    podListeners.forEach {
                        it.onEvent(mapWatchPodResponse(event))
                    }
                }
            }catch (e: RuntimeException){
                println("Pod watch ended")
            }
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

    private fun mapWatchJobResponse(response: Watch.Response<V1Job>) =
        resourceEvent(response.type) { response.`object`.snapshot(it) }

    private fun mapWatchPodResponse(response: Watch.Response<V1Pod>) =
        resourceEvent(response.type) { response.`object`.snapshot(it) }

    override fun getLogs(pod: PodReference): String? {
        return coreV1Api
            .readNamespacedPodLog(
                pod.name, pod.namespace, null, null, null, null, null, null, null, null, null
            )
    }

    override fun stopListeners() {
        podWatch?.close()
        jobWatch?.close()
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