package bachelor.executor.old

import bachelor.executor.old.api.*
import bachelor.core.api.snapshot.JobReference
import bachelor.core.api.snapshot.ExecutionSnapshot
import bachelor.core.executor.ClientException
import bachelor.core.executor.JobExecutionRequest
import bachelor.core.executor.JobExecutor
import bachelor.core.executor.ServerException
import org.apache.logging.log4j.LogManager
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class   ImperativeJobExecutor(
    private val client: JobApiClient,
) : JobExecutor {

    private val logger = LogManager.getLogger()


    override fun execute(request: JobExecutionRequest): ExecutionSnapshot {
        var job: JobReference? = null
        try {
            job = client.createJob(request.jobSpec)
            waitUntilReady(job, request.isRunningTimeout)
            waitUntilTerminated(job, request.isTerminatedTimeout)

            val status = client.getJobStatus(job)

            verifySucceeded(status)
            return null!!
//            return status.logs?.let { String(it) }
        } catch (e: ClientException) {
            logger.error("Client Exception occurred during execution of '${request}'", e)
            throw e
        } catch (e: Exception) {
            logger.error("Calculation failed unexpectedly during execution of '${request}'", e)
            if (e is ServerException) throw e
            throw ServerException(
                "Calculation '${request}' failed unexpectedly\n${e.javaClass.simpleName}: ${e.message}",
                e
            )
        } finally {
            job?.let { client.deleteJob(it) }
        }
    }

    private fun waitUntilTerminated(job: JobReference, terminationTimeout: Duration) {
        val future = client.informOnTerminated(job)
        try {
            future.get(terminationTimeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: TimeoutException) {
            future.cancel(true)
            val status = client.getJobStatus(job)
            throw CalculationTerminationTimeoutException("Job was not terminated within a specified deadline ($terminationTimeout ms).\n\nJob Status:\n$status")
        }
    }

    private fun waitUntilReady(job: JobReference, readyTimeout: Duration) {
        val future = client.informOnReadyToReadLogs(job)
        try {
            future.get(readyTimeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: TimeoutException) {
            future.cancel(true)
            val status = client.getJobStatus(job)
            throw JobReadyTimeoutException("Job was not ready within a specified deadline ($readyTimeout ms).\n\nJob Status:\n$status")
        }
    }

    private fun verifySucceeded(status: JobStatus) {
        if (status.containerExitCode == null) {
            throw IllegalStateException("Invalid status code.\nJob Status:\n$status")
        }
        if (status.containerExitCode != 0) {
            throw CalculationTerminatedWithErrorException("Calculation exited with a non-zero code.\nJob Status:\n$status")
        }
    }

}