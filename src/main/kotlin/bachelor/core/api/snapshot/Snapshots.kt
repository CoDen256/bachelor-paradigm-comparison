package bachelor.core.api.snapshot

sealed interface Snapshot

/**
 * Job snapshot represents a set of properties of a particular job at a
 * particular point in time.
 */
sealed interface JobSnapshot: Snapshot

/**
 * [InitialJobSnapshot] represents an empty or initial job snapshot, when
 * the job has not been created yet and no information is available.
 */
object InitialJobSnapshot : JobSnapshot {
    override fun toString(): String = "JOB-INIT"
}

/**
 * Active job snapshot represents a snapshot of an active/created job. The
 * underlying job contains certain information about its status and state:
 * - name: the name of the job
 * - conditions: set of job conditions, that can be either true or false
 * - status: a set of numerical properties, describing the state of a job
 *   (amount of active/running/failed/successful pods)
 */
data class ActiveJobSnapshot(val name: String,
                             val uid: String,
                             val namespace: String,
                             val conditions: List<JobCondition>,
                             val status: JobStatus,
                             val lastAction: String="NOOP") : JobSnapshot {
    val trueConditions: List<JobCondition> get() = conditions.filter { it.status.lowercase() == "true" }
    //TODO: MAYBE REMOVE ACTION FROM THE SNAPSHOT?
    override fun toString(): String {
        return "$status${if (trueConditions.isNotEmpty()) trueConditions.map { it.type } else "" }"
        return "Job($name/$status${trueConditions.map { it.type }})[${lastAction.take(1)}]"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ActiveJobSnapshot) return false
        // TODO: uid and namespace are omitted
        if (name != other.name) return false
        if (conditions != other.conditions) return false
        if (lastAction != other.lastAction) return false
        return status == other.status
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + conditions.hashCode()
        result = 31 * result + lastAction.hashCode()
        result = 31 * result + status.hashCode()
        return result
    }

    fun reference(): JobReference {
        return JobReference(name, uid, namespace)
    }

}

/**
 * A job condition represents the status of the job, that may or may not be
 * true.
 */
data class JobCondition(val status: String, val message: String, val type: String, val reason: String) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is JobCondition) return false
        // TODO: verify message?
        if (status != other.status) return false
        return type == other.type
    }

    override fun hashCode(): Int {
        var result = status.hashCode()
        result = 31 * result + type.hashCode()
        return result
    }
}

/**
 * Job status encapsulates several numeric properties about the job: the
 * amount of active, ready, failed and succeeded pods.
 */
data class JobStatus(val active: Int?, val ready: Int?, val failed: Int?, val succeeded: Int?) {
    override fun toString(): String {
        return "${active.toString()[0]}${ready.toString()[0]}${failed.toString()[0]}${succeeded.toString()[0]}"
        return "A:$active/R:$ready/F:$failed/S:$succeeded"
    }
}


/**
 * [PodSnapshot] represents a state of a pod at a particular point in time,
 * containing all information about is status and state.
 */
sealed interface PodSnapshot: Snapshot

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
data class ActivePodSnapshot(
    val name: String,
    val uid: String,
    val namespace: String,
    val controllerUid: String,
    val mainContainerState: ContainerState,
    val phase: Phase,
    val lastAction: String = "NOOP"
) : PodSnapshot {

    override fun toString(): String {
        return "$phase/$mainContainerState"
        return "Pod($name/$phase/$mainContainerState)[${lastAction.take(1)}]"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ActivePodSnapshot) return false
        // TODO: uid and namespace are omitted

        if (name != other.name) return false
        if (mainContainerState != other.mainContainerState) return false
        if (lastAction != other.lastAction) return false
        return phase == other.phase
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + mainContainerState.hashCode()
        result = 31 * result + lastAction.hashCode()
        result = 31 * result + phase.hashCode()
        return result
    }

    fun reference(): PodReference {
        return PodReference(name, uid, controllerUid, namespace)
    }

}

enum class Phase{
    PENDING,
    RUNNING,
    FAILED,
    SUCCEEDED,
    UNKNOWN
}

sealed interface ContainerState

data class WaitingState(val reason: String, val message: String) : ContainerState {
    override fun toString(): String = "Waiting($reason:$message)"
}

data class RunningState(val startedAt: String) : ContainerState {
    override fun toString(): String = "Running($startedAt)"
}

data class TerminatedState(val reason: String, val message: String, val exitCode: Int) : ContainerState {
    override fun toString(): String = "Terminated($exitCode|$reason:$message)"
}

object UnknownState : ContainerState {
    override fun toString(): String = "Unknown"
}