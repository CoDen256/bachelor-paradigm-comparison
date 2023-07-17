package bachelor.core.utils.generate

import bachelor.core.api.snapshot.ContainerState
import bachelor.core.api.snapshot.Phase
import bachelor.core.api.snapshot.Snapshot
import bachelor.core.api.snapshot.UnknownState
import bachelor.core.api.Action
import bachelor.core.api.ResourceEvent
// TODO: extract to class, with field containing job and pod name
// EVENT GENERATION
fun <T : Snapshot> noop() = ResourceEvent<T>(Action.NOOP, null)

fun <T: Snapshot> add(snapshot: T) =
    ResourceEvent(Action.ADD, snapshot)

fun <T: Snapshot> upd (snapshot: T) =
    ResourceEvent(Action.UPDATE, snapshot)

fun <T: Snapshot> del(snapshot: T) =
    ResourceEvent(Action.DELETE, snapshot)


fun add(phase: Phase, targetState: ContainerState = UnknownState, name: String = TARGET_POD) =
    ResourceEvent(Action.ADD, newPod(Action.ADD, name, phase, TARGET_JOB, targetState))

fun upd(phase: Phase, targetState: ContainerState = UnknownState, name: String = TARGET_POD) =
    ResourceEvent(Action.UPDATE, newPod(Action.UPDATE, name, phase, TARGET_JOB, targetState))

fun del(phase: Phase, targetState: ContainerState = UnknownState, name: String = TARGET_POD) =
    ResourceEvent(Action.DELETE, newPod(Action.DELETE, name, phase, TARGET_JOB, targetState))

fun add(active: Int?, ready: Int?, failed: Int?, succeeded: Int?, conditions: List<String> = listOf(), name: String = TARGET_JOB) =
    ResourceEvent(Action.ADD, newJob(Action.ADD, name, active, ready, failed, succeeded, conditions))

fun upd(active: Int?, ready: Int?, failed: Int?, succeeded: Int?, conditions: List<String> = listOf(), name: String = TARGET_JOB) =
    ResourceEvent(Action.UPDATE, newJob(Action.UPDATE, name, active, ready, failed, succeeded, conditions))

fun del(active: Int?, ready: Int?, failed: Int?, succeeded: Int?, conditions: List<String> = listOf(), name: String = TARGET_JOB) =
    ResourceEvent(Action.DELETE, newJob(Action.DELETE, name, active, ready, failed, succeeded, conditions))