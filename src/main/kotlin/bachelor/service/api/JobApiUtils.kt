package bachelor.service.api

import bachelor.reactive.kubernetes.events.Action
import bachelor.service.api.snapshot.*
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.batch.v1.Job
import io.fabric8.kubernetes.api.model.ContainerState as KubernetesContainerState
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition as KubernetesJobCondition

/** Creates a snapshot from the [Job] **/
fun Job.snapshot(lastAction: Action = Action.NOOP) = ActiveJobSnapshot(this, lastAction.toString())
/** Creates a snapshot from the [Pod] **/
fun Pod.snapshot(lastAction: Action = Action.NOOP) = ActivePodSnapshot(this, lastAction.toString())

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
                terminated.reason ?: "",
                terminated.message ?: "",
                terminated.exitCode
            )

            running != null -> RunningState(
                running.startedAt ?: "",
            )

            waiting != null -> WaitingState(
                waiting.reason ?: "",
                waiting.message ?: "",
            )

            else -> UnknownState
        }
    }
}

fun prettyString(s: ContainerState): String {
    return when (s) {
        is WaitingState -> "WAITING\n" +
                "Reason: ${s.reason}\n" +
                "Message: ${s.message}"

        is RunningState -> "RUNNING\n" +
                "Started At: ${s.startedAt}"

        is TerminatedState -> "TERMINATED\n" +
                "Exit Code: ${s.exitCode}\n" +
                "Reason: ${s.reason}\n" +
                "Message: ${s.message}"

        UnknownState -> "<UNKNOWN STATE>"
    }
}

fun prettyString(s: ActivePodSnapshot): String {
    return "Last operation: ${s.lastAction.lowercase()}\n" +
            "Name: ${s.name}\n" +
            "Phase: ${s.phase.uppercase()}\n" +
            "State: ${prettyString(s.mainContainerState)}"
}

fun prettyString(podSnapshot: PodSnapshot): String {
    return when (podSnapshot) {
        is InitialPodSnapshot -> "[Initial Pod State]: Pod is unavailable or has not been created yet"
        is ActivePodSnapshot -> prettyString(podSnapshot)
    }
}

fun prettyString(s: ActiveJobSnapshot): String {
    return "Last operation: ${s.lastAction.lowercase()}\n" +
            "Name: ${s.name}\n" +
            "Conditions: ${s.conditions}\n" +
            "Active: ${s.status.active}\n" +
            "Ready: ${s.status.ready}\n" +
            "Failed: ${s.status.failed}\n" +
            "Succeeded: ${s.status.succeeded}"
}

fun prettyString(jobSnapshot: JobSnapshot): String {
    return when (jobSnapshot) {
        is InitialJobSnapshot -> "[Initial Job State]: Job is unavailable or has not been created yet"
        is ActiveJobSnapshot -> prettyString(jobSnapshot)
    }
}

fun prettyString(snapshot: ExecutionSnapshot): String {
    val logs = snapshot.logs.content
    val podSnapshot = snapshot.podSnapshot
    val jobSnapshot = snapshot.jobSnapshot
    return "Latest Job State:\n${prettyString(jobSnapshot)}\n\n" +
            "Latest Pod State:\n${prettyString(podSnapshot)}\n\n" +
            "Logs: ${logs?.let { "\n$it" } ?: "<NO LOGS AVAILABLE>"}"
}