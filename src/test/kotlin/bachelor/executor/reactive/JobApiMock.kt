package bachelor.executor.reactive

import bachelor.core.api.JobApi
import bachelor.core.api.ResourceEventHandler
import bachelor.core.api.snapshot.ActiveJobSnapshot
import bachelor.core.api.snapshot.ActivePodSnapshot
import bachelor.core.api.snapshot.JobReference
import bachelor.core.api.snapshot.PodReference
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers

class JobApiMock(
    private val podEvents: Flux<ResourceEvent<ActivePodSnapshot>>,
    private val jobEvents: Flux<ResourceEvent<ActiveJobSnapshot>>,
) : JobApi {


    override fun addPodEventHandler(listener: ResourceEventHandler<ActivePodSnapshot>) {
        podEvents.subscribeOn(Schedulers.parallel())
            .subscribe { listener.onEvent(it) }
    }

    override fun addJobEventHandler(listener: ResourceEventHandler<ActiveJobSnapshot>) {
        jobEvents.subscribeOn(Schedulers.parallel())
            .subscribe { listener.onEvent(it) }
    }

    override fun removePodEventHandler(listener: ResourceEventHandler<ActivePodSnapshot>) {
        TODO("Not yet implemented")
    }

    override fun removeJobEventHandler(listener: ResourceEventHandler<ActiveJobSnapshot>) {
        TODO("Not yet implemented")
    }

    override fun startListeners() {
        TODO("Not yet implemented")
    }

    override fun create(spec: String): JobReference {
        TODO("Not yet implemented")
    }

    override fun delete(job: JobReference) {
        TODO("Not yet implemented")
    }

    override fun getLogs(pod: PodReference): String {
        TODO("Not yet implemented")
    }

    override fun stopListeners() {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }
}