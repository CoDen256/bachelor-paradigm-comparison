package bachelor.core.utils


import bachelor.core.api.snapshot.*
import bachelor.core.utils.generate.DelayedEmitterBuilder
import bachelor.executor.reactive.Action
import bachelor.executor.reactive.ResourceEvent
import org.junit.jupiter.api.Assertions
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.time.Duration

const val TARGET_JOB = "target-job" // fake job id
const val TARGET_POD = "target-pod" // fake pod id

// EVENTS
fun <T : Snapshot> noop() = ResourceEvent<T>(Action.NOOP, null)

fun add(phase: String, targetState: ContainerState = UnknownState, name: String = TARGET_POD) =
    ResourceEvent(Action.ADD, newPod(Action.ADD, name, phase, TARGET_JOB, targetState))

fun upd(phase: String, targetState: ContainerState = UnknownState, name: String = TARGET_POD) =
    ResourceEvent(Action.UPDATE, newPod(Action.UPDATE, name, phase, TARGET_JOB, targetState))

fun del(phase: String, targetState: ContainerState = UnknownState, name: String = TARGET_POD) =
    ResourceEvent(Action.DELETE, newPod(Action.DELETE, name, phase, TARGET_JOB, targetState))

fun add(active: Int?, ready: Int?, failed: Int?, succeeded: Int?, conditions: List<String> = listOf(), name: String = TARGET_JOB) =
    ResourceEvent(Action.ADD, newJob(Action.ADD, name, active, ready, failed, succeeded, conditions))

fun upd(active: Int?, ready: Int?, failed: Int?, succeeded: Int?, conditions: List<String> = listOf(), name: String = TARGET_JOB) =
    ResourceEvent(Action.UPDATE, newJob(Action.UPDATE, name, active, ready, failed, succeeded, conditions))

fun del(active: Int?, ready: Int?, failed: Int?, succeeded: Int?, conditions: List<String> = listOf(), name: String = TARGET_JOB) =
    ResourceEvent(Action.DELETE, newJob(Action.DELETE, name, active, ready, failed, succeeded, conditions))



// JOB SNAPSHOT
fun inactiveJobSnapshot(name: String = TARGET_JOB) =
    inactiveJob(name)

fun activeJobSnapshot(name: String = TARGET_JOB) =
    activeJob(name)

fun runningJobSnapshot(name: String = TARGET_JOB) =
    runningJob(name)

fun succeededJobSnapshot(name: String = TARGET_JOB) =
    succeededJob(name)

fun failedJobSnapshot(name: String = TARGET_JOB) =
    failedJob(name)


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
    return failedPod(name, job, code)
}

fun successfulSnapshot(name: String = TARGET_POD, job: String = TARGET_JOB, code: Int = 0): ActivePodSnapshot {
    return successfulPod(name, job, code)
}

fun runningSnapshot(name: String = TARGET_POD, job: String = TARGET_JOB): ActivePodSnapshot {
    return runningPod(name, job)
}

fun waitingSnapshot(name: String = TARGET_POD, job: String = TARGET_JOB): ActivePodSnapshot {
    return waitingPod(name, job)
}

fun unknownSnapshot(name: String = TARGET_POD, job: String = TARGET_JOB): ActivePodSnapshot {
    return unknownPod(name, job)
}


// POD
fun failedPod(name: String = TARGET_POD, job: String = TARGET_JOB, code: Int = 1): ActivePodSnapshot {
    return newPod(name, "Failed", job, containerStateTerminated(code))
}

fun successfulPod(name: String = TARGET_POD, job: String = TARGET_JOB, code: Int = 0): ActivePodSnapshot {
    return newPod(name, "Success", job, containerStateTerminated(code))
}

fun runningPod(name: String = TARGET_POD, job: String = TARGET_JOB): ActivePodSnapshot {
    return newPod(name, "Running", job, containerStateRunning())
}

fun waitingPod(name: String = TARGET_POD, job: String = TARGET_JOB): ActivePodSnapshot {
    return newPod(name, "Pending", job, containerStateWaiting())
}

fun unknownPod(name: String = TARGET_POD, job: String = TARGET_JOB): ActivePodSnapshot {
    return newPod(name, "Pending", job, UnknownState)
}

fun containerStateWaiting(reason: String = "", message: String = "") = WaitingState(reason, message)
fun containerStateRunning(startedAt: String = "0000") =  RunningState(startedAt)
fun containerStateTerminated(code: Int, reason: String = "", message: String = "") = TerminatedState(reason, message, code)

fun newPod(name: String, phase: String, job: String, state: ContainerState = UnknownState): ActivePodSnapshot{
    return newPod(Action.NOOP, name, phase, job, state)
}

fun newPod(action: Action, name: String, phase: String, job: String, state: ContainerState = UnknownState): ActivePodSnapshot {
    return ActivePodSnapshot(name, name, "", job, state, phase, action.name)
}


fun newJob(action: Action, name: String, active: Int? = null, ready: Int? = null, failed: Int? = null, succeeded: Int? = null,
           conditions: List<String> = listOf()
): ActiveJobSnapshot {
    return ActiveJobSnapshot(
        name,
        name,
        "",
        conditions.map { JobCondition("True", "", it, "") },
        JobStatus(active, ready, failed, succeeded),
        action.name
    )
}

fun newJob(name: String, active: Int? = null, ready: Int? = null, failed: Int? = null, succeeded: Int? = null,
           conditions: List<String> = listOf()
): ActiveJobSnapshot {
    return newJob(Action.NOOP, name, active, ready, failed, succeeded, conditions)
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
private fun <T: Snapshot> parseEvent(event: String, snapshot: String, elementParser: (String, Action) -> T): ResourceEvent<T> {
    val type = event[0]
    val action = when (type) {
        'A' -> Action.ADD
        'U' -> Action.UPDATE
        'D' -> Action.DELETE
        else -> throw IllegalArgumentException("Cannot parse event: $event")
    }
    return ResourceEvent(action, elementParser(snapshot, action))
}

/**
 * Parse job of format {ACTIVE}{READY}{FAILED}{SUCCESS}
 */
private fun parseJob(snapshot: String, action: Action): ActiveJobSnapshot {
    val active = parseIntOrNull(snapshot[0])
    val ready = parseIntOrNull(snapshot[1])
    val failed = parseIntOrNull(snapshot[2])
    val successful = parseIntOrNull(snapshot[3])
    return newJob(action, TARGET_JOB, active, ready, failed, successful)
}

/**
 * Parse pod of Format {PHASE}/{CONTAINER_STATE}[{EXIT_CODE}]
 */
private fun parsePod(snapshot: String, action: Action): ActivePodSnapshot {
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
        'U' -> UnknownState
        'W' -> containerStateWaiting()
        'R' -> containerStateRunning()
        'T' -> containerStateTerminated(Integer.parseInt(containerStateString.substring(1)))
        else -> throw IllegalArgumentException("Unknown Container State for $containerStateString")
    }
    return newPod(action, TARGET_POD, phase, TARGET_JOB, containerState)
}

private fun parseIntOrNull(property: Char): Int?{
    if (property == 'n') return null
    return Integer.parseInt(property.toString())
}

/**
 * Parse events of format {EVENT}({ELEMENT})|{EVENT}({ELEMENT})|{EVENT}({ELEMENT})
 */
private fun <T: Snapshot> parseEvents(timeline: String, elementParser: (String, Action) -> T): List<ResourceEvent<T>>{
    val events = timeline
        .trim('|')
        .split("|")

    return events.map {
        val trimmed = it.trim('-')
        if (trimmed.isBlank()) return@map ResourceEvent(Action.NOOP, null)
        val jobSnapshot = trimmed.substring(2, trimmed.length-1)
        parseEvent(trimmed, jobSnapshot, elementParser)
    }
}

fun parseJobEvents(timeline: String) = parseEvents(timeline) { snapshot, action -> parseJob(snapshot, action) }
fun parsePodEvents(timeline: String) = parseEvents(timeline) { snapshot, action -> parsePod(snapshot, action) }