package bachelor.executor.reactive

import bachelor.core.api.*
import bachelor.core.api.snapshot.*
import bachelor.core.api.snapshot.Phase.*
import bachelor.core.executor.*
import bachelor.core.impl.template.*
import bachelor.core.utils.*
import bachelor.core.utils.generate.*
import bachelor.executor.reactive.Action.*
import bachelor.millis
import bachelor.verifyError
import org.apache.logging.log4j.LogManager
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.kotlin.core.publisher.toMono
import reactor.test.StepVerifier
import java.time.Duration

/**
 * Tests pertaining [KubernetesJobExecutor.executeUntilTerminated] method
 * Each test represents a timeline of pod and job events. Each column of
 * a timeline represents a delay/interval of a fixed amount of time. Each
 * event represents an action and a resource to which action occurs. The
 * format of an event: {ACTION}({RESOURCE})
 *
 * Possible actions are: (A)DD, (U)PDATE, (D)ELETE or "-" for no action
 * (NOOP)
 *
 * Possible Resources are: Pods and Jobs Format of a Job:
 * {ACTIVE}{READY}{FAILED}{SUCCEEDED}, where each property can be (0), (1)
 * or (n)ull Format of a Pod: {PHASE}/{MAIN_CONTAINER_STATE}[{EXIT_CODE}]
 * Possible phases are: (P)ending, (R)unning, (S)ucceeded, or (F)ailed
 * Possible main container states : (U)nknown, (W)aiting, (R)unning,
 * (T)erminated{EXIT_CODE}
 *
 * Example: Time: |100ms--|300ms--| Jobs: |A(nn10)|-------| Pods:
 * |A(P/W)-|U(F/T(0)| After 100ms delay, there will be emitted two events:
 * Add-Event for a pod in phase Pending and in the Waiting state AND
 * Add-Event for a job, which has null active, null ready, 1 failed and 0
 * succeeded pods. After 300ms from initial start, there will be emitted
 * a noop operation for a job (that is, no event) and an Update-Event
 * for a pod in phase Failed and state Terminated with 0 exit code.
 */
@ExtendWith(MockitoExtension::class)
class ReactiveJobExecutorExecuteTest {

    private val logger = LogManager.getLogger()

    private val spec = "spec"
    private val events: MutableList<Pair<Long, String>> =
        ArrayList() // events: Time since subscription in millis and description of the event
    private val originalJob = JobReference(TARGET_JOB, TARGET_JOB, "-")

    @Mock
    lateinit var api: ReactiveJobApi

    private fun run(isRunningTimeout: Duration, isTerminatedTimeout: Duration): Mono<ExecutionSnapshot> {
        return ReactiveJobExecutor(api)
            .executeUntilTerminated(JobExecutionRequest(spec, isRunningTimeout, isTerminatedTimeout))
    }

    private fun setupApi(
        jobStream: Flux<ResourceEvent<ActiveJobSnapshot>>,
        podStream: Flux<ResourceEvent<ActivePodSnapshot>>
    ) {
        // actual subscription to the stream happens a bit later, so events are shifted in time for the Executor than the time presented in the timelines
        // some of the events will already be emitted before the subscription
        whenever(api.create(spec)).thenReturn(originalJob.toMono())
        whenever(api.jobEvents()).thenReturn(jobStream)
        whenever(api.podEvents()).thenReturn(podStream)
    }

    @AfterEach
    fun tearDown() {
        logTimedEvents(events)
    }

    @Test
    fun executeAndFailToCreateAJob() {
        // SETUP
        whenever(api.jobEvents()).thenReturn(Flux.never())
        whenever(api.podEvents()).thenReturn(Flux.never())
        whenever(api.create(spec)).thenReturn(Mono.error(JobAlreadyExistsException("Job already exists", null)))

        // EXERCISE
        val result = run(millis(100), millis(100))

        // VERIFY
        StepVerifier.create(result).verifyError<JobAlreadyExistsException> {}

    }

    @Test
    fun executeAndFailToLoadJob() {
        // SETUP
        whenever(api.jobEvents()).thenReturn(Flux.never())
        whenever(api.podEvents()).thenReturn(Flux.never())
        whenever(api.create(spec)).thenReturn(Mono.error(InvalidJobSpecException("Job spec is invalid", null)))

        // EXERCISE
        val result = run(millis(5000), millis(5000))

        // VERIFY
        StepVerifier.create(result).verifyError<InvalidJobSpecException> { }
    }

    @Test
    fun executeAndEmitErrorInStream() {
        // SETUP
        val jobSink = Sinks.many().multicast().onBackpressureBuffer<ResourceEvent<ActiveJobSnapshot>>()
        val jobStream = jobSink.asFlux()
        val podStream = Flux.empty<ResourceEvent<ActivePodSnapshot>>()

        setupApi(jobStream, podStream)
        jobSink.tryEmitError(IllegalStateException())

        // EXERCISE
        val result = run(millis(5000), millis(5000))

        // VERIFY
        StepVerifier.create(result).verifyError<IllegalStateException> { }

        verify(api).create(spec)
        verify(api).delete(originalJob)
    }

    /**
     * Job delay: +0ms, Pod delay: +50ms, interval:
     * 100ms T: |100ms!!|200ms--|300ms--| J:
     * |A(nnnn)|U(10nn)|-------| P: |A(P/U)-|U(P/U)-|U(P/W)-|
     */
    @Test
    fun executeAndTimeout() {
        // SETUP
        val (jobStream, podStream) = emitEventsAndLog(
            0, 50, 100,
            "|A(nnnn)|U(10nn)|-------|",
            "|A(P/U)-|U(P/U)-|U(P/W)-|"
        )

        setupApi(jobStream, podStream)

        // EXERCISE
        val result = run(millis(5000), millis(5000))

        // VERIFY
        StepVerifier.create(result).verifyTimeout(millis(100))
        verify(api).create(spec)
        verify(api).delete(originalJob)
    }

    /**
     * Job delay: +100ms, Pod delay: +180ms, interval: 100ms T:
     * |0ms----|200ms--|400ms--|600ms--|800ms--|1000ms!!|1200ms-|1400ms--|1600ms-|1800ms-|1900ms-|2000ms|
     * J:
     * |A(nnnn)|U(10nn)|-------|-------|U(11nn)|--------|U(10nn)|-------|U(n0n1)|D(n0n1)|-------|-------|
     * P:
     * |A(P/U)-|U(P/U)-|U(P/W)-|U(R/R)-|-------|U(R/T0)-|-------|U(S/T0)|-------|-------|U(S/T0)|D(S/T0)|
     */
    @Test
    fun executeAndSucceed_ThenSuccessful() {
        // SETUP
        val expectedJob = newJob(UPDATE, TARGET_JOB, 1, 1, null, null)
        val expectedPod = newPod(UPDATE, TARGET_POD, RUNNING, TARGET_JOB, containerStateTerminated(0))
        val expectedLogs = "HUSTENSAFT"

        val (jobStream, podStream) = emitEventsAndLog(
            100, 120, 200,
            "A(nnnn)|U(10nn)|-------|-------|U(11nn)|--------|U(10nn)|-------|U(n0n1)|D(n0n1)|-------|-------",
            "A(P/U)-|U(P/U)-|U(P/W)-|U(R/R)-|-------|U(R/T0)-|-------|U(S/T0)|-------|-------|U(S/T0)|D(S/T0)"
        )

        setupApi(jobStream, podStream)
        whenever(api.getLogs(any())).thenReturn(expectedLogs.toMono())

        // EXERCISE
        val result = run(millis(5000), millis(5000))

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs(expectedLogs), expectedJob, expectedPod))
            .verifyComplete()
        verify(api).create(spec)
        verify(api).delete(originalJob)
    }

    /**
     * Job delay: +100ms, Pod delay: +120ms, interval: 200ms T:
     * |0ms----|200ms--|400ms--|600ms--|800ms--|1000ms!!|1200ms-|1400ms--|1600ms-|1800ms-|1900ms-|2000ms-|
     * J:
     * |-------|A(nnnn)|U(10nn)|-------|-------|U(11nn)|--------|U(10nn)-|-------|U(n01n)|D(n01n)|-------|
     * P:
     * |-------|A(P/U)-|U(P/U)-|U(P/W)-|U(R/R)-|-------|U(R/T1)-|--------|U(S/T1)|-------|-------|U(S/T1)|
     */
    @Test
    fun executeAndFail_ThenFailure() {
        // SETUP
        val expectedJobSnapshot = newJob(UPDATE, TARGET_JOB, 1, 1, null, null)
        val expectedPodSnapshot =
            newPod(UPDATE, TARGET_POD, RUNNING, TARGET_JOB, containerStateTerminated(1))
        val expectedLogs = "HUSTEN..."

        val (jobStream, podStream) = emitEventsAndLog(
            100, 120, 200,
            "|A(nnnn)|U(10nn)|------|------|U(11nn)|-------|U(10nn)|-------|U(n01n)|D(n01n)|-------",
            "|A(P/U)-|U(P/U)-|U(P/W)|U(R/R)|-------|U(R/T1)|-------|U(S/T1)|-------|-------|U(S/T1)"
        )

        setupApi(jobStream, podStream)
        whenever(api.getLogs(any())).thenReturn(expectedLogs.toMono())

        // EXERCISE
        val result = run(millis(5000), millis(5000))

        // VERIFY
        StepVerifier.create(result)
            .verifyError<PodTerminatedWithErrorException> {
                assertEquals(
                    1,
                    ((it.currentState.podSnapshot as ActivePodSnapshot).mainContainerState as TerminatedState).exitCode
                )
                assertEquals(
                    ExecutionSnapshot(Logs(expectedLogs), expectedJobSnapshot, expectedPodSnapshot),
                    it.currentState
                )
            }

        verify(api).create(spec)
        verify(api).delete(originalJob)
    }

    /**
     * Job delay: +100ms, Pod delay: +180ms, interval: 100ms T:
     * |0ms----|100ms--|200ms--|300ms--|400ms--|500ms!!!|600ms--|700ms--|800ms--|900ms--|1000ms-|1100ms-|
     * J:
     * |-------|A(nnnn)|-------|-------|U(10nn)|-------|--------|U(n0n1)|D(n0n1)|-------|-------|
     * P:
     * |-------|-------|A(P/U)-|U(P/U)-|U(P/W)-|U(P/T0)|U(S/T0)-|-------|-------|U(S/T0)|D(S/T0)|
     */
    @Test
    fun executeAndSucceedSkipRunning_ThenSuccessful() {
        // SETUP
        val expectedJobSnapshot = newJob(UPDATE, TARGET_JOB, 1, 0, null, null)
        val expectedPodSnapshot =
            newPod(UPDATE, TARGET_POD, PENDING, TARGET_JOB, containerStateTerminated(0))
        val expectedLogs = "HUSTENSAFT"

        val (jobStream, podStream) = emitEventsAndLog(
            100, 180, 100,
            "|A(nnnn)|-------|-------|U(10nn)|-------|--------|U(n0n1)|D(n0n1)|-------|-------|",
            "|-------|A(P/U)-|U(P/U)-|U(P/W)-|U(P/T0)|U(S/T0)-|-------|-------|U(S/T1)|D(S/T1)|"
        )

        setupApi(jobStream, podStream)
        whenever(api.getLogs(any())).thenReturn(expectedLogs.toMono())

        // EXERCISE
        val result = run(millis(5000), millis(5000))

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs(expectedLogs), expectedJobSnapshot, expectedPodSnapshot))
            .verifyComplete()
        verify(api).create(spec)
        verify(api).delete(originalJob)
    }

    /**
     * Job delay: +100ms, Pod delay: +180ms, interval: 100ms T:
     * |0ms----|100ms--|200ms--|300ms--|400ms--|500ms!!!|600ms--|700ms--|800ms--|900ms--|1000ms-|1100ms-|
     * J:
     * |-------|A(nnnn)|-------|-------|U(10nn)|-------|--------|U(n01n)|D(n01n)|-------|-------|
     * P:
     * |-------|-------|A(P/U)-|U(P/U)-|U(P/W)-|U(P/T1)|U(F/T1)-|-------|-------|U(F/T1)|D(F/T1)|
     */
    @Test
    fun executeAndFailSkipRunning_ThenFailure() {
        // SETUP
        val expectedJob = newJob(UPDATE, TARGET_JOB, 1, 0, null, null)
        val expectedPod = newPod(UPDATE, TARGET_POD, PENDING, TARGET_JOB, containerStateTerminated(1))
        val expectedLogs = "HUSTEN...."

        val (jobStream, podStream) = emitEventsAndLog(
            100, 180, 100,
            "|A(nnnn)|-------|-------|U(10nn)|-------|--------|U(n01n)|D(n01n)|-------|-------|",
            "|-------|A(P/U)-|U(P/U)-|U(P/W)-|U(P/T1)|U(F/T1)-|-------|-------|U(F/T1)|D(F/T1)|"
        )

        setupApi(jobStream, podStream)
        whenever(api.getLogs(any())).thenReturn(expectedLogs.toMono())

        // EXERCISE
        val result = run(millis(5000), millis(5000))

        // VERIFY
        StepVerifier.create(result)
            .verifyError<PodTerminatedWithErrorException> {
                assertEquals(ExecutionSnapshot(Logs(expectedLogs), expectedJob, expectedPod), it.currentState)
            }
        verify(api).create(spec)
        verify(api).delete(originalJob)
    }

    /**
     * Job delay: +100ms, Pod delay: +100ms, interval: 100ms, Pod isRunning
     * timeout: 600ms T:
     * |0ms----|100ms--|200ms--|300ms--|400ms--|500ms--|600ms-!v|700ms--|800ms--|900ms--|
     * J:
     * |-------|A(nnnn)|U(10nn)|-------|-------|-------|--------|-------|-------|D(10nn)|
     * P:
     * |-------|-------|-------|A(P/U)-|U(P/U)-|U(P/W)-|--------|-------|-------|U(S/T0)-|
     */
    @Test
    fun executeAndFailToStart_ThenPodNotRunningTimeout() {
        // SETUP
        val expectedJobSnapshot = newJob(UPDATE, TARGET_JOB, 1, 0, null, null)
        val expectedPodSnapshot = newPod(UPDATE, TARGET_POD, PENDING, TARGET_JOB, containerStateWaiting())

        val (jobStream, podStream) = emitEventsAndLog(
            100, 100, 100,
            "|A(nnnn)|U(10nn)|-------|-------|-------|----|----|----|D(10nn)|",
            "|-------|-------|A(P/U)-|U(P/U)-|U(P/W)-|----|----|----|U(S/T0)-|"
        )

        setupApi(jobStream, podStream)
        whenever(api.getLogs(any())).thenReturn(Mono.empty())

        // EXERCISE
        val result = run(millis(600), millis(5000))

        // VERIFY
        StepVerifier.create(result)
            .verifyError<PodNotRunningTimeoutException> {
                assertEquals(ExecutionSnapshot(Logs.empty(), expectedJobSnapshot, expectedPodSnapshot), it.currentState)
            }
        verify(api).create(spec)
        verify(api).delete(originalJob)
    }

    /**
     * Job delay: +100ms, Pod delay: +100ms, interval: 100ms, Pod isRunning
     * timeout: 600ms, Pod termination timeout: 700ms T:
     * |0ms----|100ms--|200ms--|300ms--|400ms--|500ms--|600ms-!!|700ms!v|800ms--|900ms--|
     * J:
     * |-------|A(nnnn)|U(10nn)|-------|-------|-------|--------|-------|-------|D(10nn)|
     * P:
     * |-------|-------|A(P/U)-|U(P/U)-|U(P/W)-|U(R/R)-|--------|-------|-------|U(S/T0)|
     */
    @Test
    fun executeForTooLong_ThenPodNotTerminatedTimeout() {
        // SETUP
        val expectedJobSnapshot = newJob(UPDATE, TARGET_JOB, 1, 0, null, null)
        val expectedPodSnapshot = newPod(UPDATE, TARGET_POD, RUNNING, TARGET_JOB, containerStateRunning())

        val (jobStream, podStream) = emitEventsAndLog(
            100, 100, 100,
            "|A(nnnn)|U(10nn)|-------|-------|-------|--------|-------|-------|D(10nn)|",
            "|-------|A(P/U)-|U(P/U)-|U(P/W)-|U(R/R)--|-------|-------|-------|U(S/T0)|"
        )

        setupApi(jobStream, podStream)
        whenever(api.getLogs(any())).thenReturn(Mono.empty())

        // EXERCISE
        val result = run(millis(600), millis(700))

        // VERIFY
        StepVerifier.create(result)
            .verifyError<PodNotTerminatedTimeoutException> {
                assertEquals(ExecutionSnapshot(Logs.empty(), expectedJobSnapshot, expectedPodSnapshot), it.currentState)
            }
        verify(api).create(spec)
        verify(api).delete(originalJob)
    }

    /**
     * Job delay: +100ms, Pod delay: +100ms, interval: 100ms Pod running
     * timeout: 300ms T: |0ms----|100ms--|200ms!!|300ms--|400ms|500ms|600ms-v|
     * J: |-------|A(nnnn)|-------|-------|-----|-----|-------|
     * P: |-------|A(P/U)-|U(R/R)-|-------|-----|-----|U(S/T0)|
     */
    @Test
    fun executeWithNoEventAfterRunningForTooLong_ThenSuccessful() {
        // SETUP
        val expectedJob = newJob(ADD, TARGET_JOB, null, null, null, null)
        val expectedPod = newPod(UPDATE, TARGET_POD, SUCCEEDED, TARGET_JOB, containerStateTerminated(0))
        val expectedLogs = "HUSTENSAFT"

        val (jobStream, podStream) = emitEventsAndLog(
            100, 100, 100,
            "|A(nnnn)|------|----|----|----|-------|",
            "|A(P/U)-|U(R/R)|----|----|----|U(S/T0)|"
        )

        setupApi(jobStream, podStream)
        whenever(api.getLogs(any())).thenReturn(expectedLogs.toMono())

        // EXERCISE
        val result = run(millis(300), millis(1000))

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs(expectedLogs), expectedJob, expectedPod))
            .verifyComplete()

        verify(api).create(spec)
        verify(api).delete(originalJob)
    }

    private fun logTimedEvents(events: List<Pair<Long, String>>) {
        events.sortedBy { it.first }
            .forEach { logger.info(it.second) }
    }

    private fun emitEventsAndLog(
        delayJob: Long, delayPod: Long,
        interval: Long,
        timelineJob: String, timelinePod: String
    ): Pair<Flux<ResourceEvent<ActiveJobSnapshot>>, Flux<ResourceEvent<ActivePodSnapshot>>> {
        return emitJobEvents(delayJob, interval, timelineJob) to
                emitPodEvents(delayPod, interval, timelinePod)
    }

    private fun emitPodEvents(delay: Long, interval: Long, timeline: String): Flux<ResourceEvent<ActivePodSnapshot>> {
        return emitWithInterval(delay, interval, *parsePodEvents(timeline).toTypedArray())
    }

    private fun emitJobEvents(delay: Long, interval: Long, timeline: String): Flux<ResourceEvent<ActiveJobSnapshot>> {
        return emitWithInterval(delay, interval, *parseJobEvents(timeline).toTypedArray())
    }

    private fun <T : Snapshot> emitWithInterval(
        delay: Long,
        interval: Long,
        vararg eventsToEmit: ResourceEvent<T>
    ): Flux<ResourceEvent<T>> {
        return Flux.interval(millis(delay), millis(interval))
            .map { eventsToEmit[it.toInt()] }
            .timed()
            .doOnNext {
                events.add(
                    it.elapsedSinceSubscription().toMillis() to "${
                        it.elapsedSinceSubscription().toMillis()
                    } - ${it.get()}"
                )
            }
            .map { it.get() }
            .take(eventsToEmit.size.toLong())
    }
}
