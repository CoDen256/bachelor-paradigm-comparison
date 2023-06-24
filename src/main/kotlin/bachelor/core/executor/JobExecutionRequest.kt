package bachelor.core.executor

import java.time.Duration

/**
 * [JobExecutionRequest] represents a request to perform an execution of a
 * kubernetes job by [KubernetesJobExecutor]
 *
 * @property jobSpec the job spec to create and run
 * @property isRunningTimeout specifies the maximum timeout to wait until
 *     the related pod is in a running or a terminated state
 * @property isTerminatedTimeout specifies the maximum timeout until the
 *     related pod has terminated
 */
data class JobExecutionRequest(
    val jobSpec: String,
    val isRunningTimeout: Duration,
    val isTerminatedTimeout: Duration
) {
    override fun toString(): String {
        return "$isRunningTimeout|$isTerminatedTimeout|Spec: ${jobSpec.length} bytes"
    }
}
