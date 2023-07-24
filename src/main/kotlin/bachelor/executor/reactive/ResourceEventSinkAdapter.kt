package bachelor.executor.reactive

import bachelor.core.api.ResourceEvent
import bachelor.core.api.ResourceEventHandler
import bachelor.core.api.snapshot.Snapshot
import reactor.core.publisher.Sinks

class ResourceEventSinkAdapter <S: Snapshot>(private val sink: Sinks.Many<ResourceEvent<S>>)
    : ResourceEventHandler<S> {
    override fun onEvent(event: ResourceEvent<S>) {
        sink.tryEmitNext(event)
    }

    override fun onError(t: Throwable) {
        sink.tryEmitError(t)
    }

    override fun close() {
        sink.tryEmitComplete()
    }
}