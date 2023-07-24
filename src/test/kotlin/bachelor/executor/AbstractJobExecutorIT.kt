package bachelor.executor

import bachelor.awaitNoPodsPresent
import bachelor.core.ImageRunRequest
import bachelor.core.KubernetesBasedImageRunner
import bachelor.core.NAMESPACE
import bachelor.core.api.JobApi
import bachelor.core.api.snapshot.*
import bachelor.core.api.snapshot.Phase.PENDING
import bachelor.core.api.snapshot.Phase.RUNNING
import bachelor.core.executor.JobExecutor
import bachelor.core.executor.PodNotRunningTimeoutException
import bachelor.core.executor.PodNotTerminatedTimeoutException
import bachelor.core.executor.PodTerminatedWithErrorException
import bachelor.core.impl.api.fabric8.Fabric8JobApi
import bachelor.core.impl.template.BaseJobTemplateFiller
import bachelor.core.impl.template.JobTemplateFileLoader
import bachelor.core.utils.generate.TARGET_JOB
import bachelor.core.utils.generate.running
import bachelor.core.utils.generate.terminated
import bachelor.core.utils.generate.waiting
import bachelor.createNamespace
import bachelor.core.api.Action
import bachelor.core.api.Action.ADD
import bachelor.core.api.Action.UPDATE
import bachelor.getJobs
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import reactor.core.publisher.Mono
import java.io.File
import java.time.Duration
import kotlin.test.assertEquals


abstract class AbstractJobExecutorIT (
    val createExecutor: (JobApi) -> JobExecutor
){

    private val namespace = NAMESPACE
    private val JOB_NAME = TARGET_JOB

    private val helper = KubernetesClientBuilder().build().apply {
        createNamespace(this@AbstractJobExecutorIT.namespace)
    }
    private val resolver = BaseJobTemplateFiller()
    private val jobSpecFile = "/template/executor-test-job.yaml"
    private val jobSpecProvider = JobTemplateFileLoader(File(this::class.java.getResource(jobSpecFile)!!.toURI()))


    private lateinit var api: JobApi
    private lateinit var executor: JobExecutor
    private lateinit var runner: KubernetesBasedImageRunner

    @BeforeEach
    fun startup() {
        api = Fabric8JobApi(helper, NAMESPACE)
        executor = createExecutor(api)
        runner = KubernetesBasedImageRunner(executor, jobSpecProvider, resolver)
        helper.awaitNoPodsPresent(namespace)
    }


    @Test
    fun successful_shortExecution() {
        api.startListeners()

        val result = runner.run(
            10L, 11L,
            0, 0,
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
    fun successful_longExecution() {
        api.startListeners()

        val result = runner.run(
            10L, 11L,
            3, 0,
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
    fun failed_shortExecution() {
        api.startListeners()

        val result = try {
            runner.run(
                10L, 11L,
                0, 3,
            )
        } catch (ex: PodTerminatedWithErrorException) {
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
    fun failed_longExecution() {
        api.startListeners()

        val result = try {
            runner.run(
                10L, 11L,
                3, 3,
            )
        } catch (ex: PodTerminatedWithErrorException) {
            ex.currentState
        }


        val (podName, podUid, jobUid) = podRef(result)
        val expectedJob = job(jobUid, UPDATE, 1, 1, null, null)
        val expectedPod = pod(podName, podUid, jobUid, UPDATE, RUNNING, terminated(3, "Error"))
        val expectedLogs = Logs("start\nslept 3\nend\n")


        assertNoJobPresent()

        assertEquals(expectedJob, result.jobSnapshot)
        assertEquals(expectedPod, result.podSnapshot)
        assertEquals(expectedLogs, result.logs)

        api.stopListeners()
    }

    @Test
    fun notTerminatedTimeout() {
        api.startListeners()


        val result = assertThrows<PodNotTerminatedTimeoutException> {
            runner.run(
                4, 4,
                5, 0,
            )
        }.currentState

        val (podName, podUid, jobUid) = podRef(result)
        val expectedJob = job(jobUid, UPDATE, 1, 1, null, null)
        val expectedPod =
            pod(podName, podUid, jobUid, UPDATE, RUNNING, running((podState(result) as RunningState).startedAt))
        val expectedLogs = Logs("start\n")

        assertNoJobPresent()

        assertEquals(expectedJob, result.jobSnapshot)
        assertEquals(expectedPod, result.podSnapshot)
        assertEquals(expectedLogs, result.logs)

        api.stopListeners()
    }


    @Test
    fun notRunningTimeout() {
        api.startListeners()


        val result = assertThrows<PodNotRunningTimeoutException> {
            runner.run(
                4, 10,
                0, 0,
                failToStart = true
            )
        }.currentState

        val (podName, podUid, jobUid) = podRef(result)
        val expectedJob = job(jobUid, UPDATE, 1, 0, null, null)
        val expectedPod = pod(
            podName,
            podUid,
            jobUid,
            UPDATE,
            PENDING,
            waiting("ErrImagePull", (podState(result) as WaitingState).message)
        )
        val expectedLogs = Logs.empty()

        assertNoJobPresent()

        assertEquals(expectedJob, result.jobSnapshot)
        assertEquals(expectedPod, result.podSnapshot)
        assertEquals(expectedLogs, result.logs)

        api.stopListeners()
    }

    @Test
    fun notRunningTimeout_tooLow() {
        api.startListeners()


        val result = assertThrows<PodNotRunningTimeoutException> {
            runner.run(
                0, 10,
                0, 0,
            )
        }.currentState

        val (_, uid) = jobRef(result)
        val expectedJob = job(uid, ADD, null, null, null, null)
        val expectedPod = InitialPodSnapshot
        val expectedLogs = Logs.empty()

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

    private fun podState(result: ExecutionSnapshot): ContainerState {
        return (result.podSnapshot as ActivePodSnapshot).mainContainerState
    }

    private fun jobRef(result: ExecutionSnapshot): JobReference {
        return (result.jobSnapshot as ActiveJobSnapshot).reference()
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