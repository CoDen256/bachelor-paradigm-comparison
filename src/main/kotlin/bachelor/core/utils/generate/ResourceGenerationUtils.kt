package bachelor.core.utils.generate

import bachelor.core.api.snapshot.*
import bachelor.executor.reactive.Action


const val TARGET_JOB = "fake-job" // fake job id
const val TARGET_POD = "fake-pod" // fake pod id


// JOB
fun inactiveJob(name: String = TARGET_JOB) =
    newJob(name, null, null, null, null)

fun activeJob(name: String = TARGET_JOB) =
    newJob(name, 1, 0, null, null)

fun runningJob(name: String = TARGET_JOB) =
    newJob(name, 1, 1, null, null)

fun succeededJob(name: String = TARGET_JOB) =
    newJob(name, null, 0, null, 1)

fun failedJob(name: String = TARGET_JOB) =
    newJob(name, null, 0, 1, null)


fun newJob(
    name: String, active: Int? = null, ready: Int? = null, failed: Int? = null, succeeded: Int? = null,
    conditions: List<String> = listOf()
): ActiveJobSnapshot = newJob(Action.NOOP, name, active, ready, failed, succeeded, conditions)

fun newJob(
    action: Action, name: String, active: Int? = null, ready: Int? = null, failed: Int? = null, succeeded: Int? = null,
    conditions: List<String> = listOf()
): ActiveJobSnapshot = ActiveJobSnapshot(
    name,
    name,
    "",
    conditions.map { JobCondition("True", "", it, "") },
    JobStatus(active, ready, failed, succeeded),
    action.name
)


// POD
fun failedPod(name: String = TARGET_POD, job: String = TARGET_JOB, code: Int = 1): ActivePodSnapshot =
    newPod(name, Phase.FAILED, job, containerStateTerminated(code))

fun successfulPod(name: String = TARGET_POD, job: String = TARGET_JOB, code: Int = 0): ActivePodSnapshot =
    newPod(name, Phase.SUCCEEDED, job, containerStateTerminated(code))

fun runningPod(name: String = TARGET_POD, job: String = TARGET_JOB): ActivePodSnapshot =
    newPod(name, Phase.RUNNING, job, containerStateRunning())

fun waitingPod(name: String = TARGET_POD, job: String = TARGET_JOB): ActivePodSnapshot =
    newPod(name, Phase.PENDING, job, containerStateWaiting())

fun unknownPod(name: String = TARGET_POD, job: String = TARGET_JOB): ActivePodSnapshot =
    newPod(name, Phase.PENDING, job, UnknownState)

fun newPod(name: String, phase: Phase, job: String, state: ContainerState = UnknownState): ActivePodSnapshot =
    newPod(Action.NOOP, name, phase, job, state)

fun newPod(action: Action, name: String, phase: Phase, job: String, state: ContainerState = UnknownState
): ActivePodSnapshot =
    ActivePodSnapshot(name, name, "", job, state, phase, action.name)


fun containerStateWaiting(reason: String = "", message: String = "") =
    WaitingState(reason, message)
fun containerStateRunning(startedAt: String = "0000") =
    RunningState(startedAt)
fun containerStateTerminated(code: Int, reason: String = "", message: String = "") =
    TerminatedState(reason, message, code)
