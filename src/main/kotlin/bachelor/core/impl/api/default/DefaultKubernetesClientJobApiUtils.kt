package bachelor.core.impl.api.default

import bachelor.executor.reactive.Action
import bachelor.core.api.snapshot.*
import io.kubernetes.client.openapi.models.V1ContainerState as KubernetesContainerState
import io.kubernetes.client.openapi.models.V1Job as Job
import io.kubernetes.client.openapi.models.V1JobCondition as KubernetesJobCondition
import io.kubernetes.client.openapi.models.V1Pod as Pod

fun Job.snapshot(lastAction: Action = Action.NOOP) =
    ActiveJobSnapshot(
        metadata?.name ?: "[Job name unavailable]",
        metadata?.uid ?: "[Job uid unavailable]",
        metadata?.namespace ?: "[Pod namespace unavailable]",
        getJobConditions(this),
        getJobStatus(this)
    )

fun Pod.reference() = snapshot().reference()

/** Creates a snapshot from the [Pod] * */
fun Pod.snapshot(lastAction: Action = Action.NOOP) =
    ActivePodSnapshot(
        metadata?.name ?: "[Pod name unavailable]",
        metadata?.namespace ?: "[Pod namespace unavailable]",
        metadata?.labels?.get("controller-uid") ?: "[Pod namespace unavailable]",
        getMainContainerState(this),
        getPhase(this)
    )


fun getJobConditions(job: Job): List<JobCondition> {
    return job.status?.conditions?.map { mapJobCondition(it) } ?: emptyList()
}

fun getJobStatus(job: Job): JobStatus {
    return JobStatus(job.status?.active, job.status?.ready, job.status?.failed, job.status?.succeeded)
}

fun mapJobCondition(it: KubernetesJobCondition): JobCondition =
    JobCondition(it.status ?: "", it.message ?: "", it.type ?: "", it.reason ?: "")

fun getPhase(pod: Pod): String = pod.status?.phase ?: "UnknownPhase"

fun getMainContainerState(pod: Pod): ContainerState {
    return getKubernetesMainContainerState(pod)?.let { mapContainerState(it) } ?: UnknownState
}

fun getKubernetesMainContainerState(pod: Pod): KubernetesContainerState? =
    getKubernetesContainerStates(pod).firstOrNull()

fun getKubernetesContainerStates(pod: Pod): List<KubernetesContainerState> =
    pod.status?.containerStatuses?.mapNotNull { it?.state } ?: emptyList()

fun mapContainerState(status: KubernetesContainerState): ContainerState {
    return status.run {
        when {
            terminated != null -> TerminatedState(
                terminated?.reason ?: "",
                terminated?.message ?: "",
                terminated?.exitCode ?: -1
            )

            running != null -> RunningState(
                running?.startedAt.toString(),
            )

            waiting != null -> WaitingState(
                waiting?.reason ?: "",
                waiting?.message ?: "",
            )

            else -> UnknownState
        }
    }
}