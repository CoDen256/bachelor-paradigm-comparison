package bachelor.core

import bachelor.core.api.*
import bachelor.core.api.snapshot.*
import bachelor.core.executor.JobExecutionRequest
import bachelor.core.executor.JobExecutor
import bachelor.core.executor.PodNotRunningTimeoutException
import bachelor.core.utils.generate.*
import bachelor.millis
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mock
import org.mockito.invocation.InvocationOnMock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.capture
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.stubbing.OngoingStubbing
import kotlin.test.assertEquals

@ExtendWith(MockitoExtension::class)
abstract class AbstractJobExecutorTest(
    val createExecutor: (JobApi) -> JobExecutor
) {

    private val namespace = "ns"
    private val JOB_NAME = TARGET_JOB
    private val JOB_SPEC = "spec"

    @Mock
    private lateinit var api: JobApi

    @Captor
    private lateinit var jobHandlerCaptor: ArgumentCaptor<ResourceEventHandler<ActiveJobSnapshot>>

    @Captor
    private lateinit var podHandlerCaptor: ArgumentCaptor<ResourceEventHandler<ActivePodSnapshot>>

    private lateinit var executor: JobExecutor

    @BeforeEach
    fun startup() {
        executor = createExecutor(api)
    }


    @Nested
    @DisplayName("Given failed before or upon job creation When executed Then throw exception and unsubscribe")
    inner class GivenFailBeforeJobCreation{

        @AfterEach
        fun teardown() {
            verify(api).addJobEventHandler(capture(jobHandlerCaptor))
            verify(api).removeJobEventHandler(jobHandlerCaptor.value)
        }

        @Test
        fun `Given failed to add job handler Then rethrow and unsubscribe`() {
            whenever(api.addJobEventHandler(any())).thenThrow(IllegalArgumentException())


            assertThrows<IllegalArgumentException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(0)))
            }

            verify(api).removePodEventHandler(any())
        }

        @Test
        fun `Given failed to add pod handler Then rethrow and unsubscribe`() {
            whenever(api.addPodEventHandler(any())).thenThrow(IllegalStateException())

            assertThrows<IllegalStateException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(0)))
            }

            verify(api).addPodEventHandler(capture(podHandlerCaptor))
            verify(api).removePodEventHandler(podHandlerCaptor.value)
        }


        @Test
        fun `Given invalid job spec Then rethrow and unsubscribe`() {
            whenever(api.create(JOB_SPEC)).thenThrow(InvalidJobSpecException("", null))

            assertThrows<InvalidJobSpecException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(0)))
            }

            verify(api).addPodEventHandler(capture(podHandlerCaptor))
            verify(api).removePodEventHandler(podHandlerCaptor.value)

            verify(api).create(JOB_SPEC)
        }


        @Test
        fun `Given job already exists Then rethrow and unsubscribe`() {
            whenever(api.create(JOB_SPEC)).thenThrow(JobAlreadyExistsException("", null))

            assertThrows<JobAlreadyExistsException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(0)))
            }

            verify(api).addPodEventHandler(capture(podHandlerCaptor))
            verify(api).removePodEventHandler(podHandlerCaptor.value)

            verify(api).create(JOB_SPEC)
        }
    }


    @Nested
    @DisplayName("Given no pod events When executed Then throw PodNotRunningException and delete job and unsubscribe")
    inner class GivenNoPodEvents {

        private val events = ArrayList<ResourceEvent<ActiveJobSnapshot>>()
        private val jobRef = JobReference("target", "target", "ns")

        private val intermediateJobSnapshot = ActiveJobSnapshot("target", "target", "ns", listOf(), JobStatus(1, 1, 1, 1))
        private val latestSnapshot = ActiveJobSnapshot("target", "target", "ns", listOf(), JobStatus(0, 0, 0, 1))
        private val randomJobSnapshot = ActiveJobSnapshot("target", "random", "ns", listOf(), JobStatus(0, 0, 0, 1))

        @BeforeEach
        fun setup() {
            events.clear()
            whenever(api.addJobEventHandler(any())).thenWithJobHandler { handler ->
                events.forEach { handler.onEvent(it) }
            }
            whenever(api.create(JOB_SPEC)).thenReturn(jobRef)
        }

        @AfterEach
        fun teardown() {
            verify(api).addJobEventHandler(capture(jobHandlerCaptor))
            verify(api).removeJobEventHandler(jobHandlerCaptor.value)

            verify(api).addPodEventHandler(capture(podHandlerCaptor))
            verify(api).removePodEventHandler(podHandlerCaptor.value)

            verify(api).create(JOB_SPEC)
            verify(api).delete(jobRef)
        }

        @Test
        fun `Given no events Then empty snapshot`() {
            val ex = assertThrows<PodNotRunningTimeoutException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(100)))
            }
            assertEquals(emptySnapshot(), ex.currentState)
            assertEquals(millis(0), ex.timeout)
        }

        @Test
        fun `Given noop events Then empty snapshot`() {
            events.addAll(listOf(noop(), noop()))


            // execute
            val ex = assertThrows<PodNotRunningTimeoutException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(100)))
            }

            assertEquals(emptySnapshot(), ex.currentState)
            assertEquals(millis(0), ex.timeout)

        }

        @Test
        fun `Given single job event Then single job snapshot`() {
            events.add(upd(latestSnapshot))

            val ex = assertThrows<PodNotRunningTimeoutException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(100)))
            }

            assertEquals(snapshot(job = latestSnapshot), ex.currentState)
            assertEquals(millis(0), ex.timeout)
        }


        @Test
        fun `Given single non target job event Then empty snapshot`() {
            events.add(add(randomJobSnapshot))

            val ex = assertThrows<PodNotRunningTimeoutException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(100)))
            }

            assertEquals(emptySnapshot(), ex.currentState)
            assertEquals(millis(0), ex.timeout)
        }


        @Test
        fun `Given two job events Then latest job event`() {
            events.addAll(
                listOf(
                    upd(intermediateJobSnapshot),
                    upd(latestSnapshot)
                )
            )

            val ex = assertThrows<PodNotRunningTimeoutException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(100)))
            }

            assertEquals(snapshot(job = latestSnapshot), ex.currentState)
            assertEquals(millis(0), ex.timeout)
        }

        @Test
        fun `Given two non target job events Then empty snapshot`() {
            events.addAll(
                listOf(
                    upd(randomJobSnapshot),
                    add(randomJobSnapshot)
                )
            )

            val ex = assertThrows<PodNotRunningTimeoutException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(100)))
            }

            assertEquals(emptySnapshot(), ex.currentState)
            assertEquals(millis(0), ex.timeout)
        }


        @Test
        fun `Given one non-target and one target job event Then target job event`() {

            events.addAll(
                listOf(
                    upd(randomJobSnapshot),
                    add(latestSnapshot)
                )
            )

            val ex = assertThrows<PodNotRunningTimeoutException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(100)))
            }

            assertEquals(snapshot(job = latestSnapshot), ex.currentState)
            assertEquals(millis(0), ex.timeout)
        }

        @Test
        fun `Given one target and one non-target job event Then target job event`() {

            events.addAll(
                listOf(
                    add(latestSnapshot),
                    upd(randomJobSnapshot)
                )
            )

            val ex = assertThrows<PodNotRunningTimeoutException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(100)))
            }

            assertEquals(snapshot(job = latestSnapshot), ex.currentState)
            assertEquals(millis(0), ex.timeout)

        }

        @Test
        fun `Given multiple non-target job events Then empty snapshot`() {
            events.addAll(
                listOf(
                    add(randomJobSnapshot),
                    upd(randomJobSnapshot),
                    del(randomJobSnapshot),
                    upd(job("fake", "fake", Action.ADD, 1,1,1,1)),
                    del(randomJobSnapshot),
                )
            )

            val ex = assertThrows<PodNotRunningTimeoutException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(100)))
            }

            assertEquals(emptySnapshot(), ex.currentState)
            assertEquals(millis(0), ex.timeout)
        }


        @Test
        fun `Given multiple target job events Then latest snapshot`() {
            events.addAll(
                listOf(
                    add(intermediateJobSnapshot),
                    del(intermediateJobSnapshot),
                    upd(job("target", "target",  Action.ADD,1, 2, 3, 1)),
                    upd(latestSnapshot),
                )
            )

            val ex = assertThrows<PodNotRunningTimeoutException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(100)))
            }

            assertEquals(snapshot(job = latestSnapshot), ex.currentState)
            assertEquals(millis(0), ex.timeout)
        }

        @Test
        fun `Given multiple target and non-target job events Then the latest target snapshot`() {
            events.addAll(
                listOf(
                    upd(randomJobSnapshot),
                    add(intermediateJobSnapshot),
                    del(intermediateJobSnapshot),
                    upd(job("target", "target",  Action.ADD,1, 2, 3, 1)),
                    upd(randomJobSnapshot),
                    upd(latestSnapshot),
                    upd(randomJobSnapshot),
                )
            )

            val ex = assertThrows<PodNotRunningTimeoutException> {
                executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(100)))
            }

            assertEquals(snapshot(job = latestSnapshot), ex.currentState)
            assertEquals(millis(0), ex.timeout)
        }
    }

    private fun JobExecutor.execute(
        runningTimeout: Long = 10_000,
        terminatedTimeout: Long = 10_000,
    ): ExecutionSnapshot {
        return execute(
            JobExecutionRequest(
                JOB_SPEC,
                millis(runningTimeout),
                millis(terminatedTimeout)
            )
        )
    }

    private fun emptySnapshot() = ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot)
    private fun snapshot(
        logs: Logs = Logs.empty(),
        job: JobSnapshot = InitialJobSnapshot,
        pod: PodSnapshot = InitialPodSnapshot
    ) = ExecutionSnapshot (logs, job, pod)

    fun job(
        name: String,
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
        conditions.map { JobCondition("True", JOB_SPEC, it, JOB_SPEC) },
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

    private fun <T> OngoingStubbing<T>.thenWithJobHandler(apply: (ResourceEventHandler<ActiveJobSnapshot>) -> Unit) {
        then {
            apply((it.arguments[0] as ResourceEventHandler<ActiveJobSnapshot>))
            null
        }
    }

    private fun <T> OngoingStubbing<T>.thenWithPodHandler(apply: (ResourceEventHandler<ActivePodSnapshot>) -> Unit) {
        then {
            apply((it.arguments[0] as ResourceEventHandler<ActivePodSnapshot>))
            null
        }
    }

    private fun <T> OngoingStubbing<T>.thenCaptureJobHandlerAndAnswer(answer: (InvocationOnMock) -> Any? = { null }): ResourceEventHandler<ActiveJobSnapshot> {
        var ret: ResourceEventHandler<ActiveJobSnapshot>? = null
        then {
            ret = (it.arguments[0] as ResourceEventHandler<ActiveJobSnapshot>)
            answer(it)
        }
        return ret!!
    }

    private fun <T> OngoingStubbing<T>.thenCapturePodHandlerAndAnswer(answer: (InvocationOnMock) -> Any? = { null }): ResourceEventHandler<ActivePodSnapshot> {
        var ret: ResourceEventHandler<ActivePodSnapshot>? = null
        then {
            ret = (it.arguments[0] as ResourceEventHandler<ActivePodSnapshot>)
            answer(it)
        }
        return ret!!
    }


}
