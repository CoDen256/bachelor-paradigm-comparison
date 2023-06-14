package calculations.runner.kubernetes.api

import calculations.runner.kubernetes.JobReference
import calculations.runner.kubernetes.JobStatus
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.batch.v1.Job
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.PodResource
import io.fabric8.kubernetes.client.dsl.ScalableResource
import org.apache.logging.log4j.LogManager
import java.util.concurrent.CompletableFuture

class LoggingJobApiClient(client: KubernetesClient): BaseJobApiClient(client) {
    private val logger = LogManager.getLogger()

    override fun createJob(jobSpec: String): JobReference {
        logger.info("Loading and creating job from the job spec")
        return super.createJob(jobSpec).also {
            logger.info("Job successfully loaded and created in namespace '${it.namespace}': ${it.name}  ${it.uid}")
        }
    }

    override fun informOnReadyToReadLogs(job: JobReference): CompletableFuture<Void> {
        logger.info("Waiting until the job '${job.name}' is not waiting")
        return super.informOnReadyToReadLogs(job).thenAccept {
            logger.info("Job '${job.name}' is not waiting")
        }
    }

    override fun getJobStatus(job: JobReference): JobStatus {
        logger.info("Getting job status for '${job.name}'")
        return super.getJobStatus(job).also {
            logger.info("Job status for job '${job.name}' fetched: ${it.jobConditions}, ${it.mainContainerState}")
            logger.debug("Job status details for job '${job.name}': {}", { it })
        }
    }

    override fun deleteJob(job: JobReference) {
        logger.info("Deleting job '${job.name}' and related pod")
        super.deleteJob(job).also {
            logger.info("Job '${job.name}' is deleted")
        }
    }

    override fun podIsCreatedAndNotWaiting(pods: List<Pod?>): Boolean {
        logger.debug("Checking pod conditions for state 'Waiting': ${pods.map { it?.status }}")
        return super.podIsCreatedAndNotWaiting(pods).also {
            logger.info("Pod readiness check ${if (it) "succeeded" else "failed"} ")
        }
    }

    override fun informOnTerminated(job: JobReference): CompletableFuture<Void> {
        logger.info("Waiting until the job '${job.name}' is not terminated")
        return super.informOnTerminated(job).thenAccept {
            logger.info("Job '${job.name}' is terminated")
        }
    }

    override fun podIsTerminated(pods: List<Pod?>): Boolean {
        logger.debug("Checking pod conditions for state 'Terminated': ${pods.map { it?.status }}")
        return super.podIsTerminated(pods).also {
            logger.info("Pod termination check ${if (it) "succeeded" else "failed"} ")
        }
    }

    override fun loadJob(jobSpec: String): ScalableResource<Job> {
        return super.loadJob(jobSpec).also {
            logger.info("Job successfully loaded.")
        }
    }

    override fun createOrReplace(jobResource: ScalableResource<Job>): Job? {
        logger.info("Creating or replacing job")
        return super.createOrReplace(jobResource).also {
            logger.info("Job created: ${it?.metadata?.name}")
            logger.debug("Job details: {}", {it})
        }
    }

    override fun readAllLogs(job: JobReference): ByteArray? {
        logger.info("Reading logs for job '${job.name}'")
        return super.readAllLogs(job).also { logs ->
            logger.info("Read logs: {} bytes", {logs ?.size ?: 0})
            logger.debug("Logs: {}", { logs?.let { String(it) } })
        }
    }

    override fun getJob(job: JobReference): Job? {
        logger.info("Getting job instance from the cluster '${job.name}' in '${job.namespace}'")
        return super.getJob(job).also {
            logger.info("Fetched job: ${it?.metadata?.name}")
            logger.debug( "Job details: {}", {it})
        }
    }

    override fun getJobResource(job: JobReference): ScalableResource<Job>? {
        logger.info("Getting job instance from the cluster '${job.name}' in '${job.namespace}'")
        return super.getJobResource(job)
    }

    override fun getPodResource(job: JobReference): PodResource? {
        logger.info("Getting job instance from the cluster '${job.name}' in '${job.namespace}'")
        return super.getPodResource(job)
    }

    override fun findPod(job: JobReference): Pod? {
        logger.info("Looking for a pod controlled by the job '${job.name}' by label '${job.uid}'")
        return super.findPod(job).also { pod ->
            logger.info(pod?.let { "Found related pod ${it.metadata?.name}" } ?: "Pod not found")
            logger.debug("Found pod: {}", { pod })
        }
    }

    override fun close() {
        logger.info("Closing the client")
        super.close()
    }
}