package bachelor.core.impl.api.fabric8

import bachelor.executor.reactive.Action
import bachelor.core.api.snapshot.*
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.batch.v1.Job
import io.fabric8.kubernetes.api.model.ContainerState as KubernetesContainerState
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition as KubernetesJobCondition

// TODO: should we put [pod/job name unavailable and co] or maybe just null?
// TODO: how to react if metadata is null?
/** Creates a snapshot from the [Job] * */
fun Job.snapshot(lastAction: Action = Action.NOOP) =
    ActiveJobSnapshot(
        metadata?.name ?: "[Job name unavailable]",
        metadata?.uid ?: "[Job uid unavailable]",
        metadata?.namespace ?: "[Pod namespace unavailable]",
        getJobConditions(this),
        getJobStatus(this),
        lastAction.name
    )

fun Pod.reference() = snapshot().reference()

/** Creates a snapshot from the [Pod] * */
fun Pod.snapshot(lastAction: Action = Action.NOOP) =
    ActivePodSnapshot(
        metadata?.name ?: "[Pod name unavailable]",
        metadata?.namespace ?: "[Pod namespace unavailable]",
        metadata?.labels?.get("controller-uid") ?: "[Pod namespace unavailable]",
        getMainContainerState(this),
        getPhase(this),
        lastAction.name
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
                running?.startedAt ?: "",
            )

            waiting != null -> WaitingState(
                waiting?.reason ?: "",
                waiting?.message ?: "",
            )

            else -> UnknownState
        }
    }
}
