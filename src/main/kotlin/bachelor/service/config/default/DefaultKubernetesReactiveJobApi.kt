package bachelor.service.config.default

import bachelor.reactive.kubernetes.events.ResourceEvent
import bachelor.service.api.ReactiveJobApi
import bachelor.service.api.resources.JobReference
import bachelor.service.api.resources.PodReference
import bachelor.service.api.snapshot.ActiveJobSnapshot
import bachelor.service.api.snapshot.ActivePodSnapshot
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.apis.BatchV1Api
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.util.Yaml
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono


class DefaultKubernetesReactiveJobApi(
    client: ApiClient,
    private val namespace: String
) : ReactiveJobApi {
    override fun startListeners() {
    }

    private val batchV1Api = BatchV1Api(client)
    private val coreV1Api = CoreV1Api(client)

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
        return Flux.empty()
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