package calculations.runner.kubernetes.api

import calculations.runner.kubernetes.*
import calculations.runner.kubernetes.ContainerState
import io.fabric8.kubernetes.api.model.*
import io.fabric8.kubernetes.api.model.batch.v1.Job
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable
import io.fabric8.kubernetes.client.dsl.PodResource
import io.fabric8.kubernetes.client.dsl.ScalableResource
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.CompletableFuture

open class BaseJobApiClient(private val client: KubernetesClient) : JobApiClient {

    override fun createJob(jobSpec: String): JobReference {
        val jobResource = loadJob(jobSpec)
        val job = createOrReplace(jobResource)!!
        return JobReference(
            job.metadata.uid,
            job.metadata.name,
            job.metadata.namespace
        )
    }

    override fun informOnReadyToReadLogs(job: JobReference): CompletableFuture<Void> {
        return selectPodByJobLabel(job)
            .informOnCondition { podIsCreatedAndNotWaiting(it) }
            .thenAccept { }
    }

    override fun informOnTerminated(job: JobReference): CompletableFuture<Void> {
        return selectPodByJobLabel(job)
            .informOnCondition { podIsTerminated(it) }
            .thenAccept { }
    }

    override fun getJobStatus(job: JobReference): JobStatus {
        val logs = readAllLogs(job)
        val jobInstance: Job? = getJob(job)
        val pod: Pod? = findPod(job)
        val mainContainerStatus = pod?.let { getMainContainerStatus(it) }
        return JobStatus(
            logs = logs,
            jobName = job.name,
            podName = pod?.metadata?.name,
            jobConditions = jobInstance?.let { getJobConditions(it) } ?: emptyList(),
            mainContainerState = mainContainerStatus?.let { getActiveContainerState(it) },
            containerExitCode = mainContainerStatus?.terminated?.exitCode
        )
    }

    override fun deleteJob(job: JobReference) {
        getPodResource(job)?.delete()
        getJobResource(job)?.delete()
    }


    protected open fun podIsCreatedAndNotWaiting(pods: List<Pod?>): Boolean =
        pods.isNotEmpty() &&
        pods.all { podIsCreated(it) && mainContainerIsNotWaiting(it!!) }

    protected open fun podIsTerminated(pods: List<Pod?>): Boolean =
        pods.isNotEmpty() &&
                pods.all { podIsCreated(it) && mainContainerIsTerminated(it!!) }


    private fun podIsCreated(pod: Pod?) = pod != null

    private fun mainContainerIsNotWaiting(pod: Pod): Boolean {
        val status = getMainContainerStatus(pod) ?: return false
        return status.waiting == null
    }

    private fun mainContainerIsTerminated(pod: Pod): Boolean {
        val status = getMainContainerStatus(pod) ?: return false
        return status.terminated != null
    }

    protected open fun createOrReplace(jobResource: ScalableResource<Job>): Job? {
        jobResource.delete()
        return jobResource.create()
    }

    protected open fun loadJob(jobSpec: String): ScalableResource<Job> =
        client.batch().v1().jobs().load(jobSpec.byteInputStream(UTF_8))


    protected open fun readAllLogs(job: JobReference): ByteArray? {
        return getPodResource(job)?.watchLog()?.output?.readAllBytes()
    }

    protected open fun getJob(job: JobReference): Job?{
        return getJobResource(job)?.get()
    }

    protected open fun getJobResource(job: JobReference): ScalableResource<Job>? =
        client.batch().v1()
            .jobs()
            .inNamespace(job.namespace)
            .withName(job.name)


    protected open fun getPodResource(job: JobReference): PodResource? {
        return findPod(job)?.let { client.pods().resource(it) }
    }

    protected open fun findPod(job: JobReference): Pod? {
        return selectPodByJobLabel(job)
            .list()
            .items
            .firstOrNull()
    }

    protected open fun selectPodByJobLabel(job: JobReference): FilterWatchListDeletable<Pod, PodList, PodResource> =
        client.pods()
            .inNamespace(job.namespace)
            .withLabel("controller-uid", job.uid)

    private fun getJobConditions(job: Job): List<JobCondition> = job.status?.conditions?.map { mapJobCondition(it) } ?: emptyList()

    private fun mapJobCondition(it: io.fabric8.kubernetes.api.model.batch.v1.JobCondition): JobCondition = JobCondition(it.status, it.message, it.type, it.reason)

    private fun getMainContainerStatus(pod: Pod): io.fabric8.kubernetes.api.model.ContainerState? = getContainerStatuses(pod).firstOrNull()

    private fun getContainerStatuses(pod: Pod): List<io.fabric8.kubernetes.api.model.ContainerState> =
        pod.status?.containerStatuses?.mapNotNull { it?.state } ?: emptyList()

    private fun getActiveContainerState(state: io.fabric8.kubernetes.api.model.ContainerState): ContainerState{
        return state.run {
            when {
                terminated != null -> ContainerState(terminated.reason, terminated.message, terminated.javaClass.simpleName)
                running != null -> ContainerState(null, "Started at: ${running.startedAt}", running.javaClass.simpleName)
                waiting != null -> ContainerState(waiting.reason, waiting.message, waiting.javaClass.simpleName)
                else -> ContainerState(null, null, null)
            }
        }
    }

    override fun close() {
        client.close()
    }
}