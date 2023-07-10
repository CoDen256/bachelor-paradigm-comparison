package bachelor.core.api

import bachelor.core.api.snapshot.Snapshot
import bachelor.executor.reactive.ResourceEvent

fun interface ResourceEventListener<S: Snapshot> {
    fun onEvent(event: ResourceEvent<S>)
}

class AggregatedEventListener<S: Snapshot>(private val listeners: Iterable<ResourceEventListener<S>>): ResourceEventListener<S> {
    override fun onEvent(event: ResourceEvent<S>) {
        listeners.forEach { it.onEvent(event) }
    }
}