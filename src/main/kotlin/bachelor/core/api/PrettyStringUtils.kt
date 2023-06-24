package bachelor.core.api

import bachelor.core.api.snapshot.*


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