package bachelor.service.config.default

import bachelor.reactive.kubernetes.events.ResourceEvent
import bachelor.service.api.ReactiveJobApi
import bachelor.service.api.resources.JobReference
import bachelor.service.api.resources.PodReference
import bachelor.service.api.snapshot.ActiveJobSnapshot
import bachelor.service.api.snapshot.ActivePodSnapshot
import com.google.gson.reflect.TypeToken
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.apis.BatchV1Api
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.util.Watch
import io.kubernetes.client.util.Yaml
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.util.concurrent.atomic.AtomicBoolean


class DefaultKubernetesClientReactiveJobApi(
    private val client: ApiClient,
    private val namespace: String
) : ReactiveJobApi {
    private val batchV1Api = BatchV1Api(client)
    private val coreV1Api = CoreV1Api(client)

    private val watchJobCall = batchV1Api.listNamespacedJobCall(namespace, null, null, null, null, null, null, null, null, null, true, null)
    private val watchPodCall = coreV1Api.listNamespacedPodCall(namespace, null, null, null, null, null, null, null, null, null, true, null)

    private var watchesStarted = AtomicBoolean()


    private var jobWatch: Watch<V1Job>? = null
    private val jobWatchResponseType = object : TypeToken<Watch.Response<V1Job>>() {}.type
    private val podWatchResponseType = object : TypeToken<Watch.Response<V1Pod>>() {}.type

    override fun startListeners() {
        if (!watchesStarted.compareAndSet(false, true)){
            error("Watches are already started!")
        }
        jobWatch = Watch.createWatch(client,watchJobCall, jobWatchResponseType)
    }

    override fun create(spec: String): Mono<JobReference> {
        val job = Yaml.load(spec) as V1Job
        return batchV1Api
            .createNamespacedJob(namespace, job, null, null, null, null)
            .toMono()
            .map { JobReference(it.metadata!!.name!!, it.metadata!!.uid!!, it.metadata!!.namespace!!) }
    }

    override fun delete(job: JobReference) {
        batchV1Api // Propagation policy background deletes the dependents as well (the pod)
            .deleteNamespacedJob(job.name, namespace, null, null, null, null, "Background", null)
    }

    override fun podEvents(): Flux<ResourceEvent<ActivePodSnapshot>> {
        return Flux.empty()
    }

    override fun jobEvents(): Flux<ResourceEvent<ActiveJobSnapshot>> {
        if (jobWatch == null) return Flux.empty()
        return Flux.fromIterable(jobWatch).map {
            null!!
        }
    }

    override fun getLogs(pod: PodReference): Mono<String> {
        return coreV1Api
            .readNamespacedPodLog(pod.name, pod.namespace,
            null, null, null, null, null, null, null, null, null)
            .toMono()
    }

    override fun stopListeners() {
    }

    override fun close() {
    }
}