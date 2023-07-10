package bachelor.core.api

import bachelor.core.api.snapshot.Snapshot
import bachelor.executor.reactive.ResourceEvent
import java.io.Closeable

fun interface ResourceEventListener<S: Snapshot>: Closeable {
    fun onEvent(event: ResourceEvent<S>)
    override fun close(){
        // no-op by default
    }
}

class AggregatedEventListener<S: Snapshot>(private val listeners: Iterable<ResourceEventListener<S>>): ResourceEventListener<S> {
    override fun onEvent(event: ResourceEvent<S>) {
        listeners.forEach { it.onEvent(event) }
    }
}