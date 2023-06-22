package bachelor.kubernetes.utils


import bachelor.reactive.kubernetes.events.Action
import bachelor.reactive.kubernetes.events.ResourceEvent
import bachelor.service.api.resources.PodReference
import bachelor.service.api.snapshot
import bachelor.service.api.snapshot.ActiveJobSnapshot
import bachelor.service.api.snapshot.ActivePodSnapshot
import bachelor.service.api.snapshot.Snapshot
import bachelor.service.utils.DelayedEmitterBuilder
import io.fabric8.kubernetes.api.model.*
import io.fabric8.kubernetes.api.model.ContainerState
import io.fabric8.kubernetes.api.model.batch.v1.Job
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition
import io.fabric8.kubernetes.api.model.batch.v1.JobStatusBuilder
import org.junit.jupiter.api.Assertions
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.lang.IllegalArgumentException
import java.time.Duration

const val TARGET_JOB = "target-job" // fake job id
const val TARGET_POD = "target-pod" // fake pod id

fun Pod.reference() = PodReference(this.metadata.name, this.metadata.namespace ?: "", this.metadata.labels["controller-uid"] ?: "")

// EVENTS
fun <T : Snapshot> noop() = ResourceEvent<T>(Action.NOOP, null)

fun add(phase: String, targetState: KubernetesResource? = null, name: String = TARGET_POD) =
    ResourceEvent(Action.ADD, newPod(name, phase, TARGET_JOB, targetState).snapshot())

fun upd(phase: String, targetState: KubernetesResource? = null, name: String = TARGET_POD) =
    ResourceEvent(Action.UPDATE, newPod(name, phase, TARGET_JOB, targetState).snapshot())

fun del(phase: String, targetState: KubernetesResource? = null, name: String = TARGET_POD) =
    ResourceEvent(Action.DELETE, newPod(name, phase, TARGET_JOB, targetState).snapshot())

fun add(active: Int?, ready: Int?, failed: Int?, succeeded: Int?, conditions: List<String> = listOf(), name: String = TARGET_JOB) =
    ResourceEvent(Action.ADD, newJob(name, active, ready, failed, succeeded, conditions).snapshot())

fun upd(active: Int?, ready: Int?, failed: Int?, succeeded: Int?, conditions: List<String> = listOf(), name: String = TARGET_JOB) =
    ResourceEvent(Action.UPDATE, newJob(name, active, ready, failed, succeeded, conditions).snapshot())

fun del(active: Int?, ready: Int?, failed: Int?, succeeded: Int?, conditions: List<String> = listOf(), name: String = TARGET_JOB) =
    ResourceEvent(Action.DELETE, newJob(name, active, ready, failed, succeeded, conditions).snapshot())



// JOB SNAPSHOT
fun inactiveJobSnapshot(name: String = TARGET_JOB) =
    inactiveJob(name).snapshot()

fun activeJobSnapshot(name: String = TARGET_JOB) =
    activeJob(name).snapshot()

fun runningJobSnapshot(name: String = TARGET_JOB) =
    runningJob(name).snapshot()

fun succeededJobSnapshot(name: String = TARGET_JOB) =
    succeededJob(name).snapshot()

fun failedJobSnapshot(name: String = TARGET_JOB) =
    failedJob(name).snapshot()


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

// POD SNAPSHOT
fun failedSnapshot(name: String = TARGET_POD, job: String = TARGET_JOB, code: Int = 1): ActivePodSnapshot {
    return failedPod(name, job, code).snapshot()
}

fun successfulSnapshot(name: String = TARGET_POD, job: String = TARGET_JOB, code: Int = 0): ActivePodSnapshot {
    return successfulPod(name, job, code).snapshot()
}

fun runningSnapshot(name: String = TARGET_POD, job: String = TARGET_JOB): ActivePodSnapshot {
    return runningPod(name, job).snapshot()
}

fun waitingSnapshot(name: String = TARGET_POD, job: String = TARGET_JOB): ActivePodSnapshot {
    return waitingPod(name, job).snapshot()
}

fun unknownSnapshot(name: String = TARGET_POD, job: String = TARGET_JOB): ActivePodSnapshot {
    return unknownPod(name, job).snapshot()
}


// POD
fun failedPod(name: String = TARGET_POD, job: String = TARGET_JOB, code: Int = 1): Pod {
    return newPod(name, "Failed", job, containerStateTerminated(code))
}

fun successfulPod(name: String = TARGET_POD, job: String = TARGET_JOB, code: Int = 0): Pod {
    return newPod(name, "Success", job, containerStateTerminated(code))
}

fun runningPod(name: String = TARGET_POD, job: String = TARGET_JOB): Pod {
    return newPod(name, "Running", job, containerStateRunning())
}

fun waitingPod(name: String = TARGET_POD, job: String = TARGET_JOB): Pod {
    return newPod(name, "Pending", job, containerStateWaiting())
}

fun unknownPod(name: String = TARGET_POD, job: String = TARGET_JOB): Pod {
    return newPod(name, "Pending", job, null)
}

fun containerStateWaiting(reason: String = "", message: String = ""): ContainerStateWaiting = ContainerStateWaiting(message, reason)
fun containerStateRunning(startedAt: String = "0000"): ContainerStateRunning = ContainerStateRunning(startedAt)
fun containerStateTerminated(code: Int, reason: String = "", message: String = "", finishedAt: String = "1111"): ContainerStateTerminated =
    ContainerStateTerminated("", code, finishedAt, message, reason, 0, "")

fun newPod(name: String, phase: String, job: String, state: KubernetesResource? = null): Pod {
    val meta = ObjectMetaBuilder()
        .withUid(name)
        .withName(name)
        .withLabels<String, String>(mapOf("controller-uid" to job))
        .build()

    val containerState: ContainerState = ContainerStateBuilder().run {
        when (state) {
            is ContainerStateWaiting -> withWaiting(state)
            is ContainerStateRunning -> withRunning(state)
            is ContainerStateTerminated -> withTerminated(state)
            else -> this
        }
    }.build()

    val containerStatus = ContainerStatusBuilder()
        .withState(containerState)
        .build()

    val status = PodStatusBuilder()
        .withPhase(phase)
        .withContainerStatuses(listOf(containerStatus))
        .build()

    return PodBuilder()
        .withMetadata(meta)
        .withStatus(status)
        .build()
}

fun newJob(name: String, active: Int? = null, ready: Int? = null, failed: Int? = null, succeeded: Int? = null,
           conditions: List<String> = listOf()
): Job {
    val meta = ObjectMetaBuilder()
        .withUid(name)
        .withName(name)
        .build()
    val status = JobStatusBuilder()
        .withActive(active)
        .withReady(ready)
        .withSucceeded(succeeded)
        .withFailed(failed)
        .withConditions(conditions.map { JobCondition("", "", "", "", "True", it) })
        .build()
    return JobBuilder()
        .withMetadata(meta)
        .withStatus(status)
        .build()
}


inline fun <reified T : Throwable> Throwable.assertError(match: (T) -> Unit) {
    Assertions.assertInstanceOf(T::class.java, this)
    match(this as T)
}

inline fun <reified T : Throwable> StepVerifier.LastStep.verifyError(crossinline match: (T) -> Unit): Duration {
    return expectErrorSatisfies {
        it.assertError(match)
    }.verify()
}

fun <T> emitter(emitter: DelayedEmitterBuilder<T>.() -> DelayedEmitterBuilder<T>): Flux<T> {
    return emitter(DelayedEmitterBuilder()).build()
}

fun <T> cachedEmitter(cache: Int, builder: DelayedEmitterBuilder<T>.() -> DelayedEmitterBuilder<T>): Flux<T> {
    return emitter(builder).cache(cache)
}

fun <T> cachedEmitter(builder: DelayedEmitterBuilder<T>.() -> DelayedEmitterBuilder<T>): Flux<T> {
    return emitter(builder).cache()
}

fun millis(millis: Long): Duration = Duration.ofMillis(millis)

/**
 * Timeline parser parses timeline of events, like
 * - |A(nnnn)|U(10nn)|-------|-------|U(11nn)|--------|U(10nn)|-------|U(n0n1)|D(n0n1)|-------|-------|
 * - |A(P/U)-|U(P/U)-|U(P/W)-|U(R/R)-|-------|U(R/T0)-|-------|U(S/T0)|-------|-------|U(S/T0)|D(S/T0)|
 *
 * Where:
 *  * A - AddEvent
 *  * U - UpdateEvent
 *  * D - DeleteEvent
 *  * Jobs - (Active/Ready/Failed/Success), can be 0, 1 or null
 *  * Pods - (Phase/MainContainerState)
 *  * Phase - (P)ending/(R)unning/((S)ucceeded|(F)ailed)
 *  * MainContainerState - (U)nknown/(W)aiting/(R)unning/(T)erminated(exitcode)
 */
private fun <T: Snapshot> parseEvent(event: String, element: T): ResourceEvent<T> {
    val type = event[0]
    return ResourceEvent(
        when(type){
            'A' -> Action.ADD
            'U' -> Action.UPDATE
            'D' -> Action.DELETE
            else ->  throw IllegalArgumentException("Cannot parse event: $event")
        }, element
    )
}

/**
 * Parse job of format {ACTIVE}{READY}{FAILED}{SUCCESS}
 */
private fun parseJob(snapshot: String): ActiveJobSnapshot {
    val active = parseIntOrNull(snapshot[0])
    val ready = parseIntOrNull(snapshot[1])
    val failed = parseIntOrNull(snapshot[2])
    val successful = parseIntOrNull(snapshot[3])
    return newJob(TARGET_JOB, active, ready, failed, successful).snapshot()
}

/**
 * Parse pod of Format {PHASE}/{CONTAINER_STATE}[{EXIT_CODE}]
 */
private fun parsePod(snapshot: String): ActivePodSnapshot {
    val split = snapshot.split("/")
    val phase = when(val phaseString = split[0]){
        "P" -> "Pending"
        "R" -> "Running"
        "S" -> "Succeeded"
        "F" -> "Failed"
        else -> throw IllegalArgumentException("Unknown Phase for $phaseString")
    }
    val containerStateString = split[1]
    val containerState = when(containerStateString[0]){
        'U' -> null
        'W' -> containerStateWaiting()
        'R' -> containerStateRunning()
        'T' -> containerStateTerminated(Integer.parseInt(containerStateString.substring(1)))
        else -> throw IllegalArgumentException("Unknown Container State for $containerStateString")
    }
    return newPod(TARGET_POD, phase, TARGET_JOB, containerState).snapshot()
}

private fun parseIntOrNull(property: Char): Int?{
    if (property == 'n') return null
    return Integer.parseInt(property.toString())
}

/**
 * Parse events of format {EVENT}({ELEMENT})|{EVENT}({ELEMENT})|{EVENT}({ELEMENT})
 */
private fun <T: Snapshot> parseEvents(timeline: String, elementParser: (String) -> T): List<ResourceEvent<T>>{
    val events = timeline
        .trim('|')
        .split("|")

    return events.map {
        val trimmed = it.trim('-')
        if (trimmed.isBlank()) return@map ResourceEvent(Action.NOOP, null)
        val jobSnapshot = trimmed.substring(2, trimmed.length-1)
        parseEvent(trimmed, elementParser(jobSnapshot))
    }
}

fun parseJobEvents(timeline: String) = parseEvents(timeline) { parseJob(it) }
fun parsePodEvents(timeline: String) = parseEvents(timeline) { parsePod(it) }