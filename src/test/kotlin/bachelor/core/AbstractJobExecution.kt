package bachelor.core

import bachelor.awaitNoPodsPresent
import bachelor.core.api.Action
import bachelor.core.api.JobApi
import bachelor.core.api.snapshot.*
import bachelor.core.executor.JobExecutor
import bachelor.core.impl.api.fabric8.Fabric8JobApi
import bachelor.core.impl.template.BaseJobTemplateFiller
import bachelor.core.impl.template.JobTemplateFileLoader
import bachelor.core.utils.generate.TARGET_JOB
import bachelor.createNamespace
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.junit.jupiter.api.BeforeEach
import reactor.core.publisher.Mono
import java.io.File
import java.time.Duration


abstract class AbstractJobExecution (
    val createExecutor: (JobApi) -> JobExecutor
){

    private val namespace = NAMESPACE
    private val JOB_NAME = TARGET_JOB

    private lateinit var api: JobApi
    private lateinit var executor: JobExecutor

    @BeforeEach
    fun startup() {
        executor = createExecutor(api)
    }


    private fun KubernetesBasedImageRunner.run(
        runningTimeout: Long,
        terminatedTimeout: Long,
        executionTime: Long,
        exitCode: Int,
        failToStart: Boolean = false
    ): ExecutionSnapshot {
        return run(
            runningTimeout,
            terminatedTimeout,
            99999, // just to be sure, that job is deleted by the runner, not by ttl
            failToStart,
            "echo start && sleep $executionTime && echo slept $executionTime && echo end && exit $exitCode"
        ).block()!!
    }


    private fun KubernetesBasedImageRunner.run(
        runningTimeout: Long,
        terminatedTimeout: Long,
        ttl: Long,
        failToRun: Boolean,
        vararg arguments: String
    ): Mono<ExecutionSnapshot> {
        return run(
            ImageRunRequest(
                JOB_NAME,
                namespace,
                "busybox${if (failToRun) "fail" else ""}:latest",
                "/bin/sh",
                ttl,
                listOf("-c") + arguments
            ),
            Duration.ofSeconds(runningTimeout),
            Duration.ofSeconds(terminatedTimeout)
        )
    }

    fun job(
        uid: String,
        action: Action,
        active: Int? = null,
        ready: Int? = null,
        failed: Int? = null,
        succeeded: Int? = null,
        conditions: List<String> = listOf()
    ): ActiveJobSnapshot = ActiveJobSnapshot(
        JOB_NAME,
        uid,
        namespace,
        conditions.map { JobCondition("True", "", it, "") },
        JobStatus(active, ready, failed, succeeded),
        action.name
    )

    fun pod(
        name: String,
        uid: String,
        jobUid: String,
        action: Action,
        phase: Phase,
        state: ContainerState = UnknownState
    ): ActivePodSnapshot =
        ActivePodSnapshot(name, uid, namespace, jobUid, state, phase, action.name)
}