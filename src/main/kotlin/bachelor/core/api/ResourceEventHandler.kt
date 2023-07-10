package bachelor.core.api

import bachelor.core.api.snapshot.Snapshot
import bachelor.executor.reactive.ResourceEvent
import java.io.Closeable

fun interface ResourceEventHandler<S: Snapshot>: Closeable {
    fun onEvent(event: ResourceEvent<S>)
    fun onError(t: Throwable){
        // no-op by default
    }
    override fun close(){
        // no-op by default
    }
}

class AggregatedEventHandler<S: Snapshot>(private val listeners: Iterable<ResourceEventHandler<S>>): ResourceEventHandler<S> {
    override fun onEvent(event: ResourceEvent<S>) {
        listeners.forEach { it.onEvent(event) }
    }
}