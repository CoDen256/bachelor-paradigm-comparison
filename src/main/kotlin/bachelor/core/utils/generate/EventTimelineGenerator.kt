package bachelor.core.utils.generate

import bachelor.core.api.snapshot.*
import bachelor.executor.reactive.Action
import bachelor.executor.reactive.ResourceEvent

// TODO: extract to class, with field containing job and pod name
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
        "P" -> Phase.PENDING
        "R" -> Phase.RUNNING
        "S" -> Phase.SUCCEEDED
        "F" -> Phase.FAILED
        "U" -> Phase.UNKNOWN
        else -> throw IllegalArgumentException("Unknown Phase for $phaseString")
    }
    val containerStateString = split[1]
    val containerState = when(containerStateString[0]){
        'U' -> UnknownState
        'W' -> waiting()
        'R' -> running()
        'T' -> terminated(Integer.parseInt(containerStateString.substring(1)))
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