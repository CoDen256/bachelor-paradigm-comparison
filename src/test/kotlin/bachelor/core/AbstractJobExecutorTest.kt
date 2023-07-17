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
import java.time.Duration
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
    @DisplayName("Given failure before or upon job creation When executed Then throw exception and unsubscribe")
    inner class GivenFailBeforeJobCreation {

        @AfterEach
        fun teardown() {
            verify(api).addJobEventHandler(capture(jobHandlerCaptor))
            verify(api).removeJobEventHandler(jobHandlerCaptor.value)
        }

        @Test
        fun `Given failed to add job handler Then rethrow and unsubscribe`() {
            whenever(api.addJobEventHandler(any())).thenThrow(IllegalArgumentException())


            assertThrows<IllegalArgumentException> {
                execute(millis(0), millis(0))
            }

            verify(api).removePodEventHandler(any())
        }

        @Test
        fun `Given failed to add pod handler Then rethrow and unsubscribe`() {
            whenever(api.addPodEventHandler(any())).thenThrow(IllegalStateException())

            assertThrows<IllegalStateException> {
                execute(millis(0), millis(0))
            }

            verify(api).addPodEventHandler(capture(podHandlerCaptor))
            verify(api).removePodEventHandler(podHandlerCaptor.value)
        }


        @Test
        fun `Given invalid job spec Then rethrow and unsubscribe`() {
            whenever(api.create(JOB_SPEC)).thenThrow(InvalidJobSpecException("", null))

            assertThrows<InvalidJobSpecException> {
                execute(millis(0), millis(0))
            }

            verify(api).addPodEventHandler(capture(podHandlerCaptor))
            verify(api).removePodEventHandler(podHandlerCaptor.value)

            verify(api).create(JOB_SPEC)
        }


        @Test
        fun `Given job already exists Then rethrow and unsubscribe`() {
            whenever(api.create(JOB_SPEC)).thenThrow(JobAlreadyExistsException("", null))

            assertThrows<JobAlreadyExistsException> {
                execute(millis(0), millis(0))
            }

            verify(api).addPodEventHandler(capture(podHandlerCaptor))
            verify(api).removePodEventHandler(podHandlerCaptor.value)

            verify(api).create(JOB_SPEC)
        }
    }


    @Nested
    @DisplayName("Given job successfully created and handlers subscribed When executed Then unsubscribe and delete job")
    inner class GivenJobSuccessfulyCreated {
        private val jobRef = JobReference("jobName", "jobUid", "ns")

        @BeforeEach
        fun setup(){
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

        @Nested
        @DisplayName("Given job events and no pod events and no logs When executed Then throw PodNotRunningException")
        inner class GivenJobEventsButNoPodEventsNoLogs {

            private val events = ArrayList<ResourceEvent<ActiveJobSnapshot>>()

            private val intermediateJobSnapshot =
                ActiveJobSnapshot("jobName", "jobUid", "ns", listOf(), JobStatus(1, 1, 1, 1))
            private val latestSnapshot = ActiveJobSnapshot("jobName", "jobUid", "ns", listOf(), JobStatus(0, 0, 0, 1))
            private val randomJobSnapshot = ActiveJobSnapshot("jobName", "random", "ns", listOf(), JobStatus(0, 0, 0, 1))

            @BeforeEach
            fun setup() {
                events.clear()
                whenever(api.addJobEventHandler(any())).thenWithJobHandler { handler ->
                    events.forEach { handler.onEvent(it) }
                }
            }

            @Test
            fun `Given no events Then empty snapshot`() {
                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(0), millis(100))
                }
                assertEquals(emptySnapshot(), ex.currentState)
                assertEquals(millis(0), ex.timeout)
            }

            @Test
            fun `Given noop job events Then empty snapshot`() {
                events.addAll(listOf(noop(), noop()))


                // execute
                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(0), millis(100))
                }

                assertEquals(emptySnapshot(), ex.currentState)
                assertEquals(millis(0), ex.timeout)

            }

            @Test
            fun `Given single job event Then single job snapshot`() {
                events.add(upd(latestSnapshot))

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(0), millis(100))
                }

                assertEquals(snapshot(job = latestSnapshot), ex.currentState)
                assertEquals(millis(0), ex.timeout)
            }


            @Test
            fun `Given single random job event Then empty snapshot`() {
                events.add(add(randomJobSnapshot))

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(0), millis(100))
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
                    execute(millis(0), millis(100))
                }

                assertEquals(snapshot(job = latestSnapshot), ex.currentState)
                assertEquals(millis(0), ex.timeout)
            }

            @Test
            fun `Given two random job events Then empty snapshot`() {
                events.addAll(
                    listOf(
                        upd(randomJobSnapshot),
                        add(randomJobSnapshot)
                    )
                )

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(0), millis(100))
                }

                assertEquals(emptySnapshot(), ex.currentState)
                assertEquals(millis(0), ex.timeout)
            }


            @Test
            fun `Given one random and one target job event Then target job event`() {

                events.addAll(
                    listOf(
                        upd(randomJobSnapshot),
                        add(latestSnapshot)
                    )
                )

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(0), millis(100))
                }

                assertEquals(snapshot(job = latestSnapshot), ex.currentState)
                assertEquals(millis(0), ex.timeout)
            }

            @Test
            fun `Given one target and one random job event Then target job event`() {

                events.addAll(
                    listOf(
                        add(latestSnapshot),
                        upd(randomJobSnapshot)
                    )
                )

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(0), millis(100))
                }

                assertEquals(snapshot(job = latestSnapshot), ex.currentState)
                assertEquals(millis(0), ex.timeout)

            }

            @Test
            fun `Given multiple random job events Then empty snapshot`() {
                events.addAll(
                    listOf(
                        add(randomJobSnapshot),
                        upd(randomJobSnapshot),
                        del(randomJobSnapshot),
                        upd(job("fake", "fake", Action.ADD, 1, 1, 1, 1)),
                        del(randomJobSnapshot),
                    )
                )

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(0), millis(100))
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
                        upd(job("jobName", "jobUid", Action.ADD, 1, 2, 3, 1)),
                        upd(latestSnapshot),
                    )
                )

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(0), millis(100))
                }

                assertEquals(snapshot(job = latestSnapshot), ex.currentState)
                assertEquals(millis(0), ex.timeout)
            }

            @Test
            fun `Given multiple target and random job events Then the latest target snapshot`() {
                events.addAll(
                    listOf(
                        upd(randomJobSnapshot),
                        add(intermediateJobSnapshot),
                        del(intermediateJobSnapshot),
                        upd(job("jobName", "jobUid", Action.ADD, 1, 2, 3, 1)),
                        upd(randomJobSnapshot),
                        upd(latestSnapshot),
                        upd(randomJobSnapshot),
                    )
                )

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(0), millis(100))
                }

                assertEquals(snapshot(job = latestSnapshot), ex.currentState)
                assertEquals(millis(0), ex.timeout)
            }
        }



        @Nested
        @DisplayName("Given no running or terminated pod events and no job events and no logs When executed Then throw PodNotRunningException")
        inner class GivenWaitingPodEventsButNoJobEventsAndNoLogs {
            private val events = ArrayList<ResourceEvent<ActivePodSnapshot>>()


            private val intermediatePodSnapshot =
                ActivePodSnapshot("podName", "podUid", "ns", "jobUid", WaitingState("", ""), Phase.UNKNOWN)
            private val latestSnapshot =
                ActivePodSnapshot("podName", "podUid", "ns", "jobUid", WaitingState("", ""), Phase.PENDING)
            private val randomPodSnapshot =
                ActivePodSnapshot("podName", "podUid", "ns", "random", WaitingState("", ""), Phase.RUNNING)

            private val podRef = latestSnapshot.reference()


            @BeforeEach
            fun setup() {
                events.clear()
                whenever(api.addPodEventHandler(any())).thenWithPodHandler { handler ->
                    events.forEach { handler.onEvent(it) }
                }
            }

            @Test
            fun `Given noop pod events Then empty snapshot`() {
                events.addAll(listOf(noop(), noop()))


                // execute
                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(0), millis(100))
                }

                assertEquals(emptySnapshot(), ex.currentState)
                assertEquals(millis(0), ex.timeout)
            }

            @Test
            fun `Given single pod event Then single pod snapshot`() {
                events.add(upd(latestSnapshot))

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(50), millis(100))
                    // TODO: edited to 50ms, because reactive runner gets timeout before the second element is emitted (even though it should be cached and the latest should be displayed)
                }

                assertEquals(snapshot(pod = latestSnapshot), ex.currentState)
                assertEquals(millis(50), ex.timeout)
                verify(api).getLogs(podRef)
            }


            @Test
            fun `Given single random pod event Then empty snapshot`() {
                events.add(add(randomPodSnapshot))

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(50), millis(100))
                }

                assertEquals(emptySnapshot(), ex.currentState)
                assertEquals(millis(50), ex.timeout)
            }


            @Test
            fun `Given two pod events Then latest pod event`() {
                events.addAll(
                    listOf(
                        upd(intermediatePodSnapshot),
                        upd(latestSnapshot)
                    )
                )

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(50), millis(100))
                }

                assertEquals(snapshot(pod = latestSnapshot), ex.currentState)
                assertEquals(millis(50), ex.timeout)
                verify(api).getLogs(podRef)
            }

            @Test
            fun `Given two random pod events Then empty snapshot`() {
                events.addAll(
                    listOf(
                        upd(randomPodSnapshot),
                        add(randomPodSnapshot)
                    )
                )

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(50), millis(100))
                }

                assertEquals(emptySnapshot(), ex.currentState)
                assertEquals(millis(50), ex.timeout)
            }


            @Test
            fun `Given one random and one target pod event Then target pod event`() {

                events.addAll(
                    listOf(
                        upd(randomPodSnapshot),
                        add(latestSnapshot)
                    )
                )

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(50), millis(100))
                }

                assertEquals(snapshot(pod = latestSnapshot), ex.currentState)
                assertEquals(millis(50), ex.timeout)
                verify(api).getLogs(podRef)
            }

            @Test
            fun `Given one target and one random pod event Then target pod event`() {

                events.addAll(
                    listOf(
                        add(latestSnapshot),
                        upd(randomPodSnapshot)
                    )
                )

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(50), millis(100))
                }

                assertEquals(snapshot(pod = latestSnapshot), ex.currentState)
                assertEquals(millis(50), ex.timeout)
                verify(api).getLogs(podRef)
            }

            @Test
            fun `Given multiple random pod events Then empty snapshot`() {
                events.addAll(
                    listOf(
                        add(randomPodSnapshot),
                        upd(randomPodSnapshot),
                        del(randomPodSnapshot),
                        upd(pod("fake", "fake", "fake", Action.ADD, Phase.RUNNING, running())),
                        del(randomPodSnapshot),
                    )
                )

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(0), millis(100))
                }

                assertEquals(emptySnapshot(), ex.currentState)
                assertEquals(millis(0), ex.timeout)
            }


            @Test
            fun `Given multiple target pod events Then latest snapshot`() {
                events.addAll(
                    listOf(
                        add(intermediatePodSnapshot),
                        del(intermediatePodSnapshot),
                        upd(pod("podName", "podUid", "jobUid", Action.ADD, Phase.RUNNING, waiting())),
                        upd(latestSnapshot),
                    )
                )

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(50), millis(100))
                }

                assertEquals(snapshot(pod = latestSnapshot), ex.currentState)
                assertEquals(millis(50), ex.timeout)
                verify(api).getLogs(podRef)

            }

            @Test
            fun `Given multiple target and random pod events Then the latest target snapshot`() {
                events.addAll(
                    listOf(
                        upd(randomPodSnapshot),
                        add(intermediatePodSnapshot),
                        del(intermediatePodSnapshot),
                        upd(pod("podName", "podUid", "jobUid", Action.ADD, Phase.RUNNING, waiting())),
                        upd(randomPodSnapshot),
                        upd(latestSnapshot),
                        upd(randomPodSnapshot),
                    )
                )

                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(50), millis(100))
                }

                assertEquals(snapshot(pod = latestSnapshot), ex.currentState)
                assertEquals(millis(50), ex.timeout)
                verify(api).getLogs(podRef)

            }
        }

        @Nested
        @DisplayName("Given logs and waiting pod events and no job events When executed Then throw PodNotRunningException")
        inner class GivenWaitingPodEventsAndLogsButNoJobEvents {
            private val events = ArrayList<ResourceEvent<ActivePodSnapshot>>()


            private val intermediatePodSnapshot =
                ActivePodSnapshot("podName", "podUid", "ns", "jobUid", WaitingState("", ""), Phase.UNKNOWN)
            private val latestSnapshot =
                ActivePodSnapshot("podName", "podUid", "ns", "jobUid", WaitingState("", ""), Phase.PENDING)
            private val randomPodSnapshot =
                ActivePodSnapshot("podName", "podUid", "ns", "random", WaitingState("", ""), Phase.RUNNING)

            private val podRef = latestSnapshot.reference()

            @BeforeEach
            fun setup() {
                events.clear()
                whenever(api.addPodEventHandler(any())).thenWithPodHandler { handler ->
                    events.forEach { handler.onEvent(it) }
                }
            }

            @Test
            fun `Given one pod event and empty logs returned Then latest snapshot `() {
                whenever(api.getLogs(podRef)).thenReturn(null)
                events.add(add(latestSnapshot))


                // execute
                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(50), millis(100))
                }

                assertEquals(snapshot(pod = latestSnapshot), ex.currentState)
                assertEquals(millis(50), ex.timeout)

                verify(api).getLogs(podRef)
            }

            @Test
            fun `Given one pod event and thrown exception upon retrieving logs Then latest snapshot with empty logs`() {
                whenever(api.getLogs(podRef)).thenThrow(IllegalStateException())
                events.add(add(latestSnapshot))


                // execute
                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(50), millis(100))
                }

                assertEquals(snapshot(pod = latestSnapshot), ex.currentState)
                assertEquals(millis(50), ex.timeout)

                verify(api).getLogs(podRef)
            }

            @Test
            fun `Given one pod event and blank logs Then latest snapshot with blank logs`() {
                whenever(api.getLogs(podRef)).thenReturn("")
                events.add(add(latestSnapshot))


                // execute
                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(50), millis(100))
                }

                assertEquals(snapshot(logs=Logs(""), pod = latestSnapshot), ex.currentState)
                assertEquals(millis(50), ex.timeout)

                verify(api).getLogs(podRef)
            }

            @Test
            fun `Given one pod event and logs Then latest snapshot with logs`() {
                whenever(api.getLogs(podRef)).thenReturn("logs")
                events.add(add(latestSnapshot))


                // execute
                val ex = assertThrows<PodNotRunningTimeoutException> {
                    execute(millis(50), millis(100))
                }

                assertEquals(snapshot(logs=Logs("logs"), pod = latestSnapshot), ex.currentState)
                assertEquals(millis(50), ex.timeout)

                verify(api).getLogs(podRef)
            }

        }

    }

    private fun execute(
        runningTimeout: Duration = millis(10_000),
        terminatedTimeout: Duration = millis(10_000),
    ): ExecutionSnapshot {
        return executor.execute(
            JobExecutionRequest(
                JOB_SPEC,
                runningTimeout,
                terminatedTimeout
            )
        )
    }

    private fun emptySnapshot() = ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot)
    private fun snapshot(
        logs: Logs = Logs.empty(),
        job: JobSnapshot = InitialJobSnapshot,
        pod: PodSnapshot = InitialPodSnapshot
    ) = ExecutionSnapshot(logs, job, pod)

    private fun job(
        name: String,
        uid: String,
        action: Action,
        active: Int? = null,
        ready: Int? = null,
        failed: Int? = null,
        succeeded: Int? = null,
        conditions: List<String> = listOf()
    ): ActiveJobSnapshot = ActiveJobSnapshot(
        name,
        uid,
        namespace,
        conditions.map { JobCondition("True", JOB_SPEC, it, JOB_SPEC) },
        JobStatus(active, ready, failed, succeeded),
        action.name
    )

    private  fun pod(
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
