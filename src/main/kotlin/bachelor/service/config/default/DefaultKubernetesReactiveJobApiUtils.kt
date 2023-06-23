//package bachelor.service.config.default
//
//import bachelor.reactive.kubernetes.events.Action
//import bachelor.service.api.snapshot.*
//import io.kubernetes.client.openapi.models.V1Job as Job
//import io.kubernetes.client.openapi.models.V1Pod as Pod
//
//
///** Creates a snapshot from the [Job] **/
//fun Job.snapshot(lastAction: Action = Action.NOOP) = ActiveJobSnapshot(this)
///** Creates a snapshot from the [Pod] **/
//fun Pod.snapshot(lastAction: Action = Action.NOOP) = ActivePodSnapshot(this)
//
//fun getJobConditions(job: Job): List<JobCondition> {
//    return job.status?.conditions?.map { mapJobCondition(it) } ?: emptyList()
//}
//
//fun getJobStatus(job: Job): JobStatus {
//    return JobStatus(job.status?.active, job.status?.ready, job.status?.failed, job.status?.succeeded)
//}
//
//fun mapJobCondition(it: io.fabric8.kubernetes.api.model.batch.v1.JobCondition): JobCondition =
//    JobCondition(it.status ?: "", it.message ?: "", it.type ?: "", it.reason ?: "")
//
//fun getPhase(pod: Pod): String = pod.status?.phase ?: "UnknownPhase"
//
//fun getMainContainerState(pod: Pod): ContainerState {
//    return getKubernetesMainContainerState(pod)?.let { mapContainerState(it) } ?: UnknownState
//}
//
//fun getKubernetesMainContainerState(pod: Pod): io.fabric8.kubernetes.api.model.ContainerState? =
//    getKubernetesContainerStates(pod).firstOrNull()
//
//fun getKubernetesContainerStates(pod: Pod): List<io.fabric8.kubernetes.api.model.ContainerState> =
//    pod.status?.containerStatuses?.mapNotNull { it?.state } ?: emptyList()
//
//fun mapContainerState(status: io.fabric8.kubernetes.api.model.ContainerState): ContainerState {
//    return status.run {
//        when {
//            terminated != null -> TerminatedState(
//                terminated.reason ?: "",
//                terminated.message ?: "",
//                terminated.exitCode
//            )
//
//            running != null -> RunningState(
//                running.startedAt ?: "",
//            )
//
//            waiting != null -> WaitingState(
//                waiting.reason ?: "",
//                waiting.message ?: "",
//            )
//
//            else -> UnknownState
//        }
//    }
//}