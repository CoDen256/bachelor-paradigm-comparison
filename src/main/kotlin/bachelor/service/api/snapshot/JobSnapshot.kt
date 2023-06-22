package bachelor.service.api.snapshot

import bachelor.service.api.getJobConditions
import bachelor.service.api.getJobStatus
import io.fabric8.kubernetes.api.model.batch.v1.Job

/**
 * Job snapshot represents a set of properties of a particular job at a
 * particular point in time.
 */
sealed interface JobSnapshot

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
data class ActiveJobSnapshot(val job: Job, val lastAction: String) : JobSnapshot {
    val name: String = job.metadata?.name ?: "[Job name not available]"
    val conditions: List<JobCondition> = getJobConditions(job)
    val trueConditions: List<JobCondition> get() = conditions.filter { it.status.lowercase() == "true" }
    val status: JobStatus = getJobStatus(job)

    override fun toString(): String {
        return "$status${if (trueConditions.isNotEmpty()) trueConditions.map { it.type } else "" }"
        return "Job($name/$status${trueConditions.map { it.type }})[${lastAction.take(1)}]"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ActiveJobSnapshot) return false

        if (lastAction != other.lastAction) return false
        if (name != other.name) return false
        if (conditions != other.conditions) return false
        return status == other.status
    }

    override fun hashCode(): Int {
        var result = lastAction.hashCode()
        result = 31 * result + name.hashCode()
        result = 31 * result + conditions.hashCode()
        result = 31 * result + status.hashCode()
        return result
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