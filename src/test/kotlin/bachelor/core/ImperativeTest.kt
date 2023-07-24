package bachelor.core

import bachelor.core.api.Action
import bachelor.core.api.JobApi
import bachelor.core.api.ResourceEvent
import bachelor.core.api.ResourceEventHandler
import bachelor.core.api.snapshot.*
import bachelor.core.executor.JobExecutionRequest
import bachelor.core.executor.JobExecutor
import bachelor.core.executor.PodNotTerminatedTimeoutException
import bachelor.core.utils.generate.*
import bachelor.executor.imperative.ImperativeJobExecutor
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
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
@ExtendWith(MockitoExtension::class)
class ImperativeTest {
    private val podEvents = ArrayList<ResourceEvent<ActivePodSnapshot>>()

    private val waitingPodSnapshot =
        ActivePodSnapshot("podName", "podUid", "ns", "jobUid", waiting(), Phase.PENDING)
    private val intermediateRunningPodSnapshot =
        ActivePodSnapshot("podName", "podUid", "ns", "jobUid", running(), Phase.PENDING)
    private val runningPodSnapshot =
        ActivePodSnapshot("podName", "podUid", "ns", "jobUid", running(), Phase.RUNNING)
    private val terminatedPodSnapshot =
        ActivePodSnapshot("podName", "podUid", "ns", "jobUid", terminated(1), Phase.RUNNING)
    private val succeededPodSnapshot =
        ActivePodSnapshot("podName", "podUid", "ns", "jobUid", terminated(0), Phase.RUNNING)


    private val randomPodSnapshot =
        ActivePodSnapshot("podName", "podUid", "ns", "random", running(), Phase.RUNNING)

    private val podRef = runningPodSnapshot.reference()


    private val interval = 100L


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
    private val jobRef = JobReference("jobName", "jobUid", "ns")

    private var name: String = ""

    @BeforeEach
    fun setup() {
        println("TEST STARTED")
        executor = ImperativeJobExecutor(api)
        podEvents.clear()
        whenever(api.create(any())).thenReturn(jobRef)
        whenever(api.addPodEventHandler(any())).thenWithPodHandler { handler ->
            emitDelayed(podEvents, handler, interval)
        }
    }
    @AfterEach
    fun teardown(){
        println("TEST ENDED: : ${name}")
        verify(api).addJobEventHandler(capture(jobHandlerCaptor))
        verify(api).removeJobEventHandler(jobHandlerCaptor.value)

        verify(api).addPodEventHandler(capture(podHandlerCaptor))
        verify(api).removePodEventHandler(podHandlerCaptor.value)

        verify(api).delete(jobRef)
        verify(api).getLogs(podRef)
    }

    @Test
    @Timeout(value = 1000, unit = TimeUnit.MILLISECONDS)
    fun `Given successfully terminated pod event Then the terminated snapshot`() {
        name = "tRT->t"
        podEvents.addAll(listOf(
            add(succeededPodSnapshot) // 0ms
        ))

        // execute

        val result = execute(millis(5000), millis(10000))


        assertEquals(snapshot(pod = succeededPodSnapshot), result)
    }

    @Test
    @Timeout(value = 1000, unit = TimeUnit.MILLISECONDS)
    fun `Given running and successfully terminated pod event Then the terminated snapshot`() {
        name = "rRntT->t"
        podEvents.addAll(listOf(
            add(runningPodSnapshot), // 0ms
            // 50ms running timeout
            noop(), // 100ms
            add(succeededPodSnapshot) // 200ms
        ))

        // execute

        val result = execute(millis(50), millis(10000))


        assertEquals(snapshot(pod = succeededPodSnapshot), result)
    }

    @Test
    fun `Given running waiting and successfully terminated pod event Then the terminated snapshot`() {
        name = "rRwTt->PNT"

        podEvents.addAll(listOf(
            add(runningPodSnapshot), // 0ms
            // running timeout
            add(waitingPodSnapshot), // 100ms
            // terminated timeout
            add(succeededPodSnapshot) // 200ms
        ))

        // execute
        val ex = assertThrows<PodNotTerminatedTimeoutException> {
            execute(millis(50), millis(140))
        }


        assertEquals(snapshot(pod = waitingPodSnapshot), ex.currentState)
        assertEquals(millis(140), ex.timeout)
    }


    @Test
    fun `Given running running and directly successfully terminated in 200ms pod event Then the terminated snapshot`() {
        name = "rt(R+T)"
        podEvents.addAll(listOf(
            // running pod is directly emitted, but during terminated should be still able to wait for 200 ms
            add(runningPodSnapshot), // 0ms
            add(succeededPodSnapshot) // 100ms
            // 200ms running timeout
            // 200ms terminated timeout
        ))

        // execute
        val result = execute(millis(200), millis(200))


        assertEquals(snapshot(pod = succeededPodSnapshot), result)
    }

    @Test
    fun `Given running directly successfully terminated in 200ms pod event Then the terminated snapshot`() {
        name = "t(R+T)"
        podEvents.addAll(listOf(
            noop(), // 0ms
            add(succeededPodSnapshot) // 100ms
            // 200ms running timeout
            // 200ms terminated timeout
        ))

        // execute
        val result = execute(millis(200), millis(200))


        assertEquals(snapshot(pod = succeededPodSnapshot), result)
    }

    @Test
    fun `Given delayed successfully terminated pod event Then the terminated snapshot`() {
        name = "rRTt -> PNT"
        podEvents.addAll(listOf(
            add(intermediateRunningPodSnapshot), // 0ms
            // 50ms running timeout
            // 90ms terminated timeout
            add(succeededPodSnapshot) // 100ms
        ))
        execute(millis(1150), millis(1190))

        Thread.sleep(5000)
//        // execute
//
//        val ex = assertThrows<PodNotTerminatedTimeoutException> {
//        }
//
//        assertEquals(snapshot(pod = intermediateRunningPodSnapshot), ex.currentState)
//        assertEquals(millis(90), ex.timeout)
    }

    private fun emitDelayed(
        resourceEvents: List<ResourceEvent<ActivePodSnapshot>>,
        handler: ResourceEventHandler<ActivePodSnapshot>,
        interval: Long
    ) {
        val localName = name
        Flux.interval(Duration.ZERO, Duration.ofMillis(interval))
            .take(resourceEvents.size.toLong())
            .map { resourceEvents[it.toInt()] }
            .subscribe {
                println("\n----------------[EMITTED $localName] $it, ${System.currentTimeMillis().toString().substring(9, 13)}  [EMITTED] ------------")
                handler.onEvent(it)
            }
    }

    private fun execute(
        runningTimeout: Duration = millis(10_000),
        terminatedTimeout: Duration = millis(10_000),
        jobSpec: String = JOB_SPEC,
    ): ExecutionSnapshot {
        println("TEST EXECUTING: $name")
        return executor.execute(
            JobExecutionRequest(
                name,
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