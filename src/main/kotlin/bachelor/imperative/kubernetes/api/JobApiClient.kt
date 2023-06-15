package bachelor.imperative.kubernetes.api

import bachelor.service.executor.ClientException
import bachelor.service.executor.ServerException
import java.util.concurrent.CompletableFuture


interface JobApiClient : AutoCloseable {
    fun createJob(jobSpec: String): JobReference
    fun informOnReadyToReadLogs(job: JobReference): CompletableFuture<Void>
    fun informOnTerminated(job: JobReference): CompletableFuture<Void>
    fun getJobStatus(job: JobReference): JobStatus
    fun deleteJob(job: JobReference)
}

data class JobReference(val uid: String, val name: String, val namespace: String)


data class JobStatus(
    val logs: ByteArray?,
    val jobName: String,
    val podName: String?,
    val containerExitCode: Int?,
    val mainContainerState: ContainerState?,
    val jobConditions: List<JobCondition>
) {
    override fun toString(): String {
        return "Job: $jobName\n" +
                "Conditions: $jobConditions\n" +
                "Pod: $podName\n" +
                "Exit code: $containerExitCode\n" +
                "Main Container State:\n$mainContainerState\n" +
                "\nLogs:\n${logs?.let { String(it) }}"
    }
}

data class ContainerState(
    val reason: String?,
    val message: String?,
    val type: String?
) {
    override fun toString(): String {
        return "State: $type\n" +
                "Message: $message\n" +
                "Reason: $reason"
    }
}

data class JobCondition(
    val status: String?,
    val message: String?,
    val type: String?,
    val reason: String?
)

class JobReadyTimeoutException(msg: String) : ServerException(msg)
class CalculationTerminationTimeoutException(msg: String) : ClientException(msg)
class CalculationTerminatedWithErrorException(msg: String) : ClientException(msg)
