package bachelor.service.api.snapshot

import bachelor.service.api.getMainContainerState
import bachelor.service.api.getPhase
import io.fabric8.kubernetes.api.model.Pod

/**
 * [PodSnapshot] represents a state of a pod at a particular point in time,
 * containing all information about is status and state.
 */
sealed interface PodSnapshot

/**
 * [InitialPodSnapshot] represents the initial/empty snapshot of a pod.
 * The snapshot corresponds to a pod, that has not been created; thus no
 * information is available.
 */
object InitialPodSnapshot : PodSnapshot {
    override fun toString(): String = "POD-INIT"
}

/**
 * [ActivePodSnapshot] represents a snapshot of an active pod, that
 * contains information about its status and states, like phase
 * or state of the main container at a particular point in time.
 */
data class ActivePodSnapshot(val pod: Pod, val lastAction: String) : PodSnapshot {
    val name: String = pod.metadata?.name ?: "[Pod name unavailable]"
    val mainContainerState: ContainerState = getMainContainerState(pod)
    val phase: String = getPhase(pod)

    override fun toString(): String {
        return "Pod($name/$phase/$mainContainerState)[${lastAction.take(1)}]"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ActivePodSnapshot) return false

        if (lastAction != other.lastAction) return false
        if (name != other.name) return false
        if (mainContainerState != other.mainContainerState) return false
        return phase == other.phase
    }

    override fun hashCode(): Int {
        var result = lastAction.hashCode()
        result = 31 * result + name.hashCode()
        result = 31 * result + mainContainerState.hashCode()
        result = 31 * result + phase.hashCode()
        return result
    }


}

sealed interface ContainerState
data class WaitingState(val reason: String, val message: String) : ContainerState {
    override fun toString(): String = "Waiting"
}

data class RunningState(val startedAt: String) : ContainerState {
    override fun toString(): String = "Running"
}

data class TerminatedState(val reason: String, val message: String, val exitCode: Int) : ContainerState {
    override fun toString(): String = "Terminated($exitCode)"
}

object UnknownState : ContainerState {
    override fun toString(): String = "Unknown"
}