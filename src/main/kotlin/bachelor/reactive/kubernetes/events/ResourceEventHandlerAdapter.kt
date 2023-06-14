package calculations.runner.kubernetes.events

import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.client.informers.ResourceEventHandler
import reactor.core.publisher.Sinks

/**
 * [ResourceEventHandlerAdapter] adapts a sink of type [Sinks.Many] to a
 * [ResourceEventHandler]. It handles all the events [ADD, UPDATE, DELETE]
 * occurring on a specific resource, by emitting [ResourceEvent]s to a
 * [Sinks.Many], which later can be used as a [ResourceEvent] publisher.
 *
 * In this way, [ResourceEventHandlerAdapter] captures all the
 * resource-specific events generated in a kubernetes cluster and streams
 * them to the subscribers of the [sink].
 *
 * The [sink] provides convenient methods to subscribe to the underlying
 * [ResourceEvent] publisher (e.g. [Sinks.Many.asFlux]), allowing to
 * subscribe to the events captured by this [ResourceEventHandler]
 */
class ResourceEventHandlerAdapter<T : HasMetadata>(private val sink: Sinks.Many<ResourceEvent<T>>) :
    ResourceEventHandler<T> {

    override fun onAdd(obj: T?) {
        sink.tryEmitNext(ResourceEvent(Action.ADD, obj))
    }

    override fun onUpdate(oldObj: T?, newObj: T?) {
        sink.tryEmitNext(ResourceEvent(Action.UPDATE, newObj))
    }

    override fun onDelete(obj: T?, deletedFinalStateUnknown: Boolean) {
        sink.tryEmitNext(ResourceEvent(Action.DELETE, obj))
    }
}

