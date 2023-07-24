package bachelor.executor.imperative

import bachelor.core.api.ResourceEvent
import bachelor.core.api.ResourceEventHandler
import bachelor.core.api.snapshot.Snapshot
import bachelor.core.currentTime

class ResourceEventCollectionAdapter<S : Snapshot>(private val collection: MutableCollection<ResourceEvent<S>>) :
    ResourceEventHandler<S> {
    override fun onEvent(event: ResourceEvent<S>) {
        println("----------------[RECEIVED] $event, ${currentTime()}  [RECEIVED] ------------\n")
        collection.add(event)
    }
}