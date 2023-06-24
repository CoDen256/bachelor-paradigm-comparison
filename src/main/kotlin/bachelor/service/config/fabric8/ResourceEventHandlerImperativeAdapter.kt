package bachelor.service.config.fabric8

import bachelor.reactive.kubernetes.Action
import bachelor.reactive.kubernetes.ResourceEvent
import bachelor.service.api.snapshot.Snapshot
import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.client.informers.ResourceEventHandler
import reactor.core.publisher.Sinks

/**
 * [ResourceEventHandlerImperativeAdapter] adapts a sink of type [Sinks.Many] to a
 * [ResourceEventHandler]. It handles all the events [ADD, UPDATE, DELETE]
 * occurring on a specific resource, by emitting [ResourceEvent]s to a
 * [Sinks.Many], which later can be used as a [ResourceEvent] publisher.
 *
 * In this way, [ResourceEventHandlerImperativeAdapter] captures all the
 * resource-specific events generated in a kubernetes cluster and streams
 * them to the subscribers of the [sink].
 *
 * The [sink] provides convenient methods to subscribe to the underlying
 * [ResourceEvent] publisher (e.g. [Sinks.Many.asFlux]), allowing to
 * subscribe to the events captured by this [ResourceEventHandler]
 */
class ResourceEventHandlerImperativeAdapter<O : HasMetadata, S : Snapshot>(
    private val events: MutableList<ResourceEvent<S>>,
    private val mapping: (O?) -> S?
) :
    ResourceEventHandler<O> {

    override fun onAdd(obj: O?) {
        events.add(ResourceEvent(Action.ADD, mapping(obj)))
    }

    override fun onUpdate(oldObj: O?, newObj: O?) {
        events.add(ResourceEvent(Action.UPDATE, mapping(newObj)))
    }

    override fun onDelete(obj: O?, deletedFinalStateUnknown: Boolean) {
        events.add(ResourceEvent(Action.DELETE, mapping(obj)))
    }
}

