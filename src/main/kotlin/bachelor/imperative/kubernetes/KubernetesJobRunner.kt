package bachelor.imperative.kubernetes

import bachelor.service.ClientException
import bachelor.service.ServerException
import bachelor.service.run.ImageRunner
import calculations.runner.kubernetes.template.JobTemplateFiller
import calculations.runner.kubernetes.template.JobTemplateProvider
import calculations.runner.run.ImageRunRequest
import org.apache.logging.log4j.LogManager
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

open class KubernetesJobRunner(
    private val client: JobApiClient,
    private val templateProvider: JobTemplateProvider,
    private val jobTemplateSubstitutor: JobTemplateFiller,
    private val readyTimeout: Long,
    private val terminationTimeout: Long,
) : AutoCloseable, ImageRunner {

    private val logger = LogManager.getLogger()

    override fun run(spec: ImageRunRequest): String? {
        logger.info("Running ${spec.name} ${spec.script} [${spec.arguments}]")

        val template = templateProvider.getTemplate()
        logger.debug("Job Template:\n$template")
        val jobSpec = jobTemplateSubstitutor.fill(template, spec)
        logger.debug("Resolved Job Spec:\n$jobSpec")

        var job: JobReference? = null
        try {
            job = client.createJob(jobSpec)
            waitUntilReady(job)
            waitUntilTerminated(job)

            val status = client.getJobStatus(job)

            verifySucceeded(status)

            return status.logs?.let { String(it) }
        } catch (e: ClientException) {
            logger.error("Client Exception occurred during execution of '${spec.name}'", e)
            throw e
        } catch (e: Exception) {
            logger.error("Calculation failed unexpectedly during execution of '${spec.name}'", e)
            if (e is ServerException) throw e
            throw ServerException(
                "Calculation '${spec.name}' failed unexpectedly\n${e.javaClass.simpleName}: ${e.message}",
                e
            )
        } finally {
            job?.let { client.deleteJob(it) }
        }
    }

    private fun waitUntilTerminated(job: JobReference) {
        val future = client.informOnTerminated(job)
        try {
            future.get(terminationTimeout, TimeUnit.MILLISECONDS)
        } catch (e: TimeoutException) {
            future.cancel(true)
            val status = client.getJobStatus(job)
            throw CalculationTerminationTimeoutException("Job was not terminated within a specified deadline ($terminationTimeout ms).\n\nJob Status:\n$status")
        }
    }

    private fun waitUntilReady(job: JobReference) {
        val future = client.informOnReadyToReadLogs(job)
        try {
            future.get(readyTimeout, TimeUnit.MILLISECONDS)
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

    override fun close() {
        client.close()
    }
}