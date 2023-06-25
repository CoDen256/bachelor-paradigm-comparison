package bachelor.core

import bachelor.core.api.ReactiveJobApi
import bachelor.core.api.snapshot.*
import bachelor.core.api.snapshot.Phase.*
import bachelor.core.executor.JobExecutor
import bachelor.core.executor.PodTerminatedWithErrorException
import bachelor.core.impl.api.fabric8.Fabric8ReactiveJobApi
import bachelor.core.impl.template.BaseJobTemplateFiller
import bachelor.core.impl.template.JobTemplateFileLoader
import bachelor.core.utils.generate.TARGET_JOB
import bachelor.core.utils.generate.terminated
import bachelor.createNamespace
import bachelor.executor.reactive.Action
import bachelor.executor.reactive.Action.UPDATE
import bachelor.executor.reactive.ReactiveJobExecutor
import bachelor.getJobs
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import java.io.File
import java.time.Duration
import kotlin.test.assertEquals


class ReactiveJobExecutionIT {

    private val namespace = NAMESPACE
    private val JOB_NAME = TARGET_JOB

    private val helper = KubernetesClientBuilder().build().apply {
        createNamespace(this@ReactiveJobExecutionIT.namespace)
    }
    private val resolver = BaseJobTemplateFiller()
    private val jobSpecFile = "/template/executor-test-job.yaml"
    private val jobSpecProvider = JobTemplateFileLoader(File(this::class.java.getResource(jobSpecFile)!!.toURI()))


    private lateinit var api: ReactiveJobApi
    private lateinit var executor: JobExecutor
    private lateinit var runner: KubernetesBasedImageRunner

    @BeforeEach
    fun startup() {
        api = Fabric8ReactiveJobApi(helper, "executor-test")
        executor = ReactiveJobExecutor(api)
        runner = KubernetesBasedImageRunner(executor, jobSpecProvider, resolver)
    }



    @Test
    fun successful_quick() {
        api.startListeners()

        val result = runner.run(
            10L, 10L,
            0, 0,
             10
        )

        val (podName, podUid, jobUid) = podRef(result)
        val expectedJob = job(jobUid, UPDATE, 1, 0, null, null)
        val expectedPod = pod(podName, podUid, jobUid, UPDATE, PENDING, terminated(0, "Completed"))
        val expectedLogs = Logs("start\nslept 0\nend\n")

        assertNoJobPresent()

        assertEquals(expectedJob, result.jobSnapshot)
        assertEquals(expectedPod, result.podSnapshot)
        assertEquals(expectedLogs, result.logs)

        api.stopListeners()
    }

    @Test
    fun successful_long() {
        api.startListeners()

        val result = runner.run(
            20L, 20L,
            3, 0,
            20
        )

        val (podName, podUid, jobUid) = podRef(result)
        val expectedJob = job(jobUid, UPDATE, 1, 1, null, null)
        val expectedPod = pod(podName, podUid, jobUid, UPDATE, RUNNING, terminated(0, "Completed"))
        val expectedLogs = Logs("start\nslept 3\nend\n")

        assertNoJobPresent()

        assertEquals(expectedJob, result.jobSnapshot)
        assertEquals(expectedPod, result.podSnapshot)
        assertEquals(expectedLogs, result.logs)

        api.stopListeners()
    }

    @Test
    fun failed_quick() {
        api.startListeners()

        val result = try {
            runner.run(
                10L, 10L,
                0, 3,
                10
            )
        }catch (ex: PodTerminatedWithErrorException){
            ex.currentState
        }


        val (podName, podUid, jobUid) = podRef(result)
        val expectedJob = job(jobUid, UPDATE, 1, 0, null, null)
        val expectedPod = pod(podName, podUid, jobUid, UPDATE, PENDING, terminated(3, "Error"))
        val expectedLogs = Logs("start\nslept 0\nend\n")

        assertNoJobPresent()

        assertEquals(expectedJob, result.jobSnapshot)
        assertEquals(expectedPod, result.podSnapshot)
        assertEquals(expectedLogs, result.logs)

        api.stopListeners()
    }

    @Test
    fun failed_long() {
        api.startListeners()

        val result = try {
            runner.run(
                20L, 20L,
                3, 3,
                20
            )
        }catch (ex: PodTerminatedWithErrorException){
            ex.currentState
        }


        val (podName, podUid, jobUid) = podRef(result)
        val expectedJob = job(jobUid, UPDATE, 1, 1, null, null)
        val expectedPod = pod(podName, podUid, jobUid, UPDATE, RUNNING, terminated(3, "Error"))
        val expectedLogs = Logs("start\nslept 0\nend\n")


        assertNoJobPresent()

        assertEquals(expectedJob, result.jobSnapshot)
        assertEquals(expectedPod, result.podSnapshot)
        assertEquals(expectedLogs, result.logs)

        api.stopListeners()
    }

    private fun assertNoJobPresent() {
        assertTrue(helper.getJobs(namespace).isEmpty())
    }

    private fun podRef(result: ExecutionSnapshot): PodReference {
        return (result.podSnapshot as ActivePodSnapshot).reference()
    }

    private fun KubernetesBasedImageRunner.run(
        runningTimeout: Long,
        terminatedTimeout: Long,
        executionTime: Long,
        exitCode: Int,
        activeDeadlineSeconds: Long
    ): ExecutionSnapshot {
        return run(
            runningTimeout,
            terminatedTimeout,
            99999, // just to be sure, that job is deleted by the runner, not by ttl
            activeDeadlineSeconds,
            "echo start && sleep $executionTime && echo slept $executionTime && echo end && exit $exitCode"
        ).block()!!
    }

    private fun KubernetesBasedImageRunner.run(
        runningTimeout: Long,
        terminatedTimeout: Long,
        ttl: Long,
        activeDeadlineSeconds: Long,
        vararg arguments: String
    ): Mono<ExecutionSnapshot> {
        return run(
            ImageRunRequest(
                JOB_NAME,
                namespace,
                "busybox:latest",
                "/bin/sh",
                ttl,
                activeDeadlineSeconds,
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