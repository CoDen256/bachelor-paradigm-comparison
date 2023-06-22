package bachelor.service.config.default

import bachelor.reactive.kubernetes.events.ResourceEvent
import bachelor.service.api.ReactiveJobApi
import bachelor.service.api.resources.JobReference
import bachelor.service.api.resources.PodReference
import bachelor.service.api.snapshot.ActiveJobSnapshot
import bachelor.service.api.snapshot.ActivePodSnapshot
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class DefaultKubernetesReactiveJobApi(

): ReactiveJobApi {
    override fun startListeners() {
        TODO("Not yet implemented")
    }

    override fun create(spec: String): Mono<JobReference> {
        TODO("Not yet implemented")
    }

    override fun delete(job: JobReference) {
        TODO("Not yet implemented")
    }

    override fun podEvents(): Flux<ResourceEvent<ActivePodSnapshot>> {
        TODO("Not yet implemented")
    }

    override fun jobEvents(): Flux<ResourceEvent<ActiveJobSnapshot>> {
        TODO("Not yet implemented")
    }

    override fun getLogs(pod: PodReference): Mono<String> {
        TODO("Not yet implemented")
    }

    override fun stopListeners() {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }
}