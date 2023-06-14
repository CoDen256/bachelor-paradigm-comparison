package bachelor.reactive.kubernetes

import bachelor.reactive.kubernetes.executor.JobExecutionRequest
import bachelor.reactive.kubernetes.executor.KubernetesJobExecutor
import calculations.runner.kubernetes.template.JobTemplateFiller
import calculations.runner.kubernetes.template.JobTemplateProvider
import calculations.runner.run.ImageRunRequest
import bachelor.service.run.ImageRunner
import bachelor.service.ClientException
import bachelor.service.ServerException
import org.apache.logging.log4j.LogManager
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * [KubernetesJobImageRunner] is an [ImageRunner] based on
 * [KubernetesJobExecutor], that runs jobs on the kubernetes cluster. The
 * runner creates a job specification based on the template and values
 * provided from the [ImageRunRequest]. Then the job spec is used to create
 * a job within a kubernetes cluster that will run the desired image. The
 * runner waits until the job is complete and reads logs produced by the
 * related pod
 *
 * @constructor Create empty Kubernetes job based image runner
 * @property jobExecutor the executor of the kubernetes jobs
 * @property templateProvider provides a template string, which will be
 *     filled using [ImageSpec]
 * @property templateFiller used to fill the template with values from
 *     [ImageSpec]
 * @property runningTimeout the timeout specifying the maximum amount of
 *     time to wait until the underlying pod is running or terminated
 * @property terminatedTimeout the timeout specifying the maximum amount of
 *     time to wait until the underlying pod is terminated
 */
class KubernetesJobImageRunner(
    private val jobExecutor: KubernetesJobExecutor,
    private val templateProvider: JobTemplateProvider,
    private val templateFiller: JobTemplateFiller,
    private val runningTimeout: Duration,
    private val terminatedTimeout: Duration
) : ImageRunner {

    private val logger = LogManager.getLogger()

    /**
     * Runs an [ImageRunRequest] based on execution of a Kubernetes Job in a
     * cluster
     * 1) Requests a template to fill
     * 2) Fills the template with values from [ImageRunRequest], producing the
     *    job spec
     * 3) Creates an [JobExecutionRequest] containing the spec, and execution
     *    parameters
     * 4) Executes [JobExecutionRequest] via [KubernetesJobExecutor] and
     *    returns the logs
     *
     * The preparation, template providing and fillings starts only upon the
     * subscription
     *
     * @param request the image to execute
     * @return logs of the executed image
     */
    override fun run(request: ImageRunRequest): String? = Mono.defer {
        logger.info("Running $request")

        val template = templateProvider.getTemplate()
        logger.debug("Job Template:\n$template")

        val jobSpec = templateFiller.fill(template, request)
        logger.debug("Resolved Job Spec:\n$jobSpec")

        val executeJobRequest = JobExecutionRequest(jobSpec, runningTimeout, terminatedTimeout)
        logger.debug("Executing request:\n$executeJobRequest")

        jobExecutor.executeAndReadLogs(executeJobRequest)
    }.onErrorMap { mapToServerException(it, request) }.block()

    private fun mapToServerException(it: Throwable, spec: ImageRunRequest): Throwable = when (it) {
        is ClientException, is ServerException -> it
        else -> ServerException("Error while running kubernetes job $spec: ${it.message}", it)
    }
}