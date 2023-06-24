package bachelor.core.impl.api.fabric8

import bachelor.executor.reactive.Action
import bachelor.executor.reactive.ResourceEvent
import bachelor.core.api.snapshot.Snapshot
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
class ResourceEventHandlerAdapter<O : HasMetadata, S : Snapshot>(
    private val events: MutableList<ResourceEvent<S>>,
    private val mapping: (O?, Action) -> S?
) :
    ResourceEventHandler<O> {

    override fun onAdd(obj: O?) {
        addEvent(Action.ADD, obj)
    }

    override fun onUpdate(oldObj: O?, newObj: O?) {
        addEvent(Action.UPDATE, newObj)

    }

    override fun onDelete(obj: O?, deletedFinalStateUnknown: Boolean) {
        addEvent(Action.DELETE, obj)
    }

    private fun addEvent(action: Action, obj: O?) {
        events.add(ResourceEvent(action, mapping(obj, action)))
    }
}

