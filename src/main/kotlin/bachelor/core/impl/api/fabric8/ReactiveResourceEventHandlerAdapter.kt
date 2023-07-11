package bachelor.core.impl.api.fabric8

import bachelor.core.api.snapshot.Snapshot
import bachelor.core.api.Action
import bachelor.core.api.ResourceEvent
import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.client.informers.ResourceEventHandler
import reactor.core.publisher.Sinks

/**
 * [ReactiveResourceEventHandlerAdapter] adapts a sink of type [Sinks.Many] to a
 * [ResourceEventHandler]. It handles all the events [ADD, UPDATE, DELETE]
 * occurring on a specific resource, by emitting [ResourceEvent]s to a
 * [Sinks.Many], which later can be used as a [ResourceEvent] publisher.
 *
 * In this way, [ReactiveResourceEventHandlerAdapter] captures all the
 * resource-specific events generated in a kubernetes cluster and streams
 * them to the subscribers of the [sink].
 *
 * The [sink] provides convenient methods to subscribe to the underlying
 * [ResourceEvent] publisher (e.g. [Sinks.Many.asFlux]), allowing to
 * subscribe to the events captured by this [ResourceEventHandler]
 */
class ReactiveResourceEventHandlerAdapter<O : HasMetadata, S : Snapshot>(
    private val sink: Sinks.Many<ResourceEvent<S>>,
    private val mapping: (O?, Action) -> S?
) :
    ResourceEventHandler<O> {

    override fun onAdd(obj: O?) {
        emitEvent(Action.ADD, obj)
    }

    override fun onUpdate(oldObj: O?, newObj: O?) {
        emitEvent(Action.UPDATE, newObj)
    }

    override fun onDelete(obj: O?, deletedFinalStateUnknown: Boolean) {
        emitEvent(Action.DELETE, obj)
    }

    private fun emitEvent(action: Action, obj: O?) {
        sink.tryEmitNext(ResourceEvent(action, mapping(obj, action)))
    }
}

