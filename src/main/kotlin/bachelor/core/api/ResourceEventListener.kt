package bachelor.core.api

import bachelor.core.api.snapshot.Snapshot
import bachelor.executor.reactive.ResourceEvent

interface ResourceEventListener<S: Snapshot> {
    fun onEvent(event: ResourceEvent<S>)
}