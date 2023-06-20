package bachelor.service.api.snapshot

/**
 * Execution snapshot or global snapshot represents a snapshot of the whole
 * system where job is being executed. The execution snapshot consists
 * of [Logs] produced by a pod, [PodSnapshot] that contains all the
 * information at particular point in time about the pod and its status and
 * [JobSnapshot] that contains all the status and state information about a
 * job at particular point in time
 */
data class ExecutionSnapshot(val logs: Logs, val jobSnapshot: JobSnapshot, val podSnapshot: PodSnapshot) {
    override fun toString(): String {
        return "$jobSnapshot + $podSnapshot + [$logs]"
    }
}