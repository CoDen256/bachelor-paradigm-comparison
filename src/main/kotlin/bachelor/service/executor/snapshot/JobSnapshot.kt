package bachelor.service.executor.snapshot

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
        return "Job($name/$status${trueConditions.map { it.type }})[${lastAction.take(1)}]"
    }
}

/**
 * A job condition represents the status of the job, that may or may not be
 * true.
 */
data class JobCondition(val status: String, val message: String, val type: String, val reason: String)

/**
 * Job status encapsulates several numeric properties about the job: the
 * amount of active, ready, failed and succeeded pods.
 */
data class JobStatus(val active: Int?, val ready: Int?, val failed: Int?, val succeeded: Int?) {
    override fun toString(): String {
        return "A:$active/R:$ready/F:$failed/S:$succeeded"
    }
}