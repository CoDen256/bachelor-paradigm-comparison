package bachelor.executor.reactive

import bachelor.cachedEmitter
import bachelor.core.api.JobApi
import bachelor.core.api.snapshot.ExecutionSnapshot
import bachelor.core.api.snapshot.InitialJobSnapshot
import bachelor.core.api.snapshot.InitialPodSnapshot
import bachelor.core.api.snapshot.Logs
import bachelor.core.executor.*
import bachelor.core.impl.template.*
import bachelor.core.utils.*
import bachelor.core.utils.generate.*
import bachelor.core.api.Action.*
import bachelor.millis
import bachelor.verifyError
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Duration
import java.util.concurrent.TimeoutException

/** Test pertaining [ReactiveJobExecutor.nextTerminatedSnapshot] method */
@ExtendWith(MockitoExtension::class)
class ReactiveJobExecutorNextTerminatedSnapshotTest {

    @Mock
    lateinit var api: JobApi


    private fun nextTerminatedSnapshot(
        stream: Flux<ExecutionSnapshot>,
        isRunningTimeout: Duration,
        isTerminatedTimeout: Duration,
        outerTimeout: Duration,
        delaySubscription: Duration = Duration.ZERO
    ): Mono<ExecutionSnapshot> {
        stream.log("ACTUAL").subscribe()
        // Trigger the hot source to emit elements independently of Runner
        // all elements emitted without delay, will be emitted on the subscription
        // only the last element will be cached

        return ReactiveJobExecutor(api)
            .nextTerminatedSnapshot(stream.delaySubscription(delaySubscription), isRunningTimeout, isTerminatedTimeout)
            .timeout(outerTimeout)

    }

    @Test
    fun singleTerminatedStateCached() {
        // SETUP
        val expected = successfulPod()
        whenever(api.getLogs(any())).thenReturn("LOGS")

        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected))
        }

        // EXERCISE
        val result = nextTerminatedSnapshot(stream, millis(100), millis(100), millis(100))

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs("LOGS"), InitialJobSnapshot, expected))
            .verifyComplete()
        verify(api).getLogs(expected.reference())
    }


    @Test
    fun initialStatesAndTerminatedStateCached() {
        val expected = successfulPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected))
        }

        // EXERCISE
        whenever(api.getLogs(any())).thenReturn("LOGS")
        val result = nextTerminatedSnapshot(stream, millis(100), millis(100), millis(100))

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs("LOGS"), InitialJobSnapshot, expected))
            .verifyComplete()
        verify(api).getLogs(expected.reference())
    }

    @Test
    fun initialStateCached_TerminatedStateDelayed() {
        // SETUP
        val expected = successfulPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(millis(500), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected))
        }
        // Two initial snapshots, emitted on the first fake subscription before the actual one
        // Successful Snapshot emitted after the actual subscription

        // EXERCISE
        whenever(api.getLogs(any())).thenReturn("LOGS")
        val result = nextTerminatedSnapshot(stream, millis(1000), millis(1000), millis(1000))

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs("LOGS"), InitialJobSnapshot, expected))
            .verifyComplete()
        verify(api).getLogs(expected.reference())
    }

    @Test
    fun initialWaitingRunningCached_TerminatedDelayed() {
        // SETUP
        val expected = successfulPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, waitingPod()))
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, runningPod()))
            emit(millis(300), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected))
        }

        // EXERCISE
        whenever(api.getLogs(any())).thenReturn("LOGS")
        val result = nextTerminatedSnapshot(stream, millis(1000), millis(1000), millis(1000))

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs("LOGS"), InitialJobSnapshot, expected))
            .verifyComplete()
        verify(api).getLogs(expected.reference())
    }

    @Test
    fun initialWaitingCached_RunningTerminatedDelayed() {
        // SETUP
        val expected = successfulPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, waitingPod()))
            emit(millis(500), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, runningPod()))
            emit(millis(500), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected))
        }

        // EXERCISE
        whenever(api.getLogs(any())).thenReturn("LOGS")
        val result = nextTerminatedSnapshot(stream, millis(2000), millis(2000), millis(2000))

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs("LOGS"), InitialJobSnapshot, expected))
            .verifyComplete()
        verify(api).getLogs(expected.reference())
    }

    @Test
    fun multipleTerminatedCached_ReturnLastTerminated() {
        // SETUP
        val expected = successfulPod(code = 0)
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, waitingPod()))
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, runningPod()))
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, successfulPod(code = 1)))
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected))
        }
        // All the emitted values are cached and emitted before the first subscription, thus
        // we want the latest of the emitted, not the first -> cache(1)

        val subscriptionDelay = millis(10) // just to be sure, the actual subscription starts
        // a little bit later, so all values without delay get emitted already


        // EXERCISE
        whenever(api.getLogs(any())).thenReturn("LOGS")
        val result = nextTerminatedSnapshot(
            stream, millis(1000), millis(1000), millis(1000),
            delaySubscription = subscriptionDelay
        )


        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs("LOGS"), InitialJobSnapshot, expected))
            .verifyComplete()
        verify(api).getLogs(expected.reference())
    }

    @Test
    fun terminatedCached_TerminatedDelayed_ReturnFirst() {
        // SETUP
        val expected = successfulPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, waitingPod()))
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, runningPod()))
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected))
            emit(millis(500), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, successfulPod()))
        }
        // We don't want to wait for the next one, and we are accepting first snapshot as the right one

        // EXERCISE
        whenever(api.getLogs(any())).thenReturn("LOGS")
        val result = nextTerminatedSnapshot(
            stream,
            millis(1000),
            millis(1000),
            millis(1000),
        )

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs("LOGS"), InitialJobSnapshot, expected))
            .verifyComplete()
        verify(api).getLogs(expected.reference())
    }


    @Test
    fun terminatedDelayed_RunningTimeout() {
        // SETUP
        val expected = successfulPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(millis(500), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected))
        }

        // EXERCISE
        val result = nextTerminatedSnapshot(stream, millis(400), millis(1000), millis(1000))

        // VERIFY
        StepVerifier.create(result)
            .verifyError<PodNotRunningTimeoutException> {
                assertEquals(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot), it.currentState)
            }
    }

    @Test
    fun terminatedDelayed_TerminatedTimeout_ContainsCachedSnapshot() {
        // SETUP
        val expected = runningPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected))
            emit(millis(500), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, successfulPod()))
        }

        // EXERCISE
        whenever(api.getLogs(any())).thenReturn("LOGS")
        val result = nextTerminatedSnapshot(stream, millis(300), millis(300), millis(1000))

        // VERIFY
        StepVerifier.create(result)
            .verifyError<PodNotTerminatedTimeoutException> {
                assertEquals(ExecutionSnapshot(Logs("LOGS"), InitialJobSnapshot, expected), it.currentState)
            }
        verify(api).getLogs(expected.reference())
    }

    @Test
    fun terminatedDelayed_TerminatedTimeout_ContainsLastCachedSnapshot() {
        // SETUP
        val expected = runningPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, waitingPod()))
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected))
            emit(millis(600), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, successfulPod()))
        }

        // EXERCISE
        whenever(api.getLogs(any())).thenReturn("LOGS")
        val result = nextTerminatedSnapshot(stream, millis(400), millis(400), millis(1000))

        // VERIFY
        StepVerifier.create(result)
            .verifyError<PodNotTerminatedTimeoutException> {
                assertEquals(ExecutionSnapshot(Logs("LOGS"), InitialJobSnapshot, expected), it.currentState)
            }
        verify(api).getLogs(expected.reference())
    }

    @Test
    fun terminatedDelayed_RunningTimeout_ContainsDelayedSnapshot() {
        // SETUP
        val expected = waitingPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(millis(300), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected))
            emit(millis(500), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, successfulPod()))
        }
        // it works without problems and cache(1), because stream delivers always the new value, because it caches the last one and
        // the .next call delivers the last one, and its called only after timeout
        // after delay of 500 ms is expired, then the next state is fetched, which is cached by the stream.

        // EXERCISE
        whenever(api.getLogs(any())).thenReturn("LOGS")
        val result = nextTerminatedSnapshot(stream, millis(400), millis(1000), millis(1000))

        // VERIFY
        StepVerifier.create(result)
            .verifyError<PodNotRunningTimeoutException> {
                assertEquals(ExecutionSnapshot(Logs("LOGS"), InitialJobSnapshot, expected), it.currentState)
            }
        verify(api).getLogs(expected.reference())
    }

    @Test
    fun terminatedDelayed_RunningTimeout_ContainsLatestDelayedSnapshot() {
        // SETUP
        val expected = waitingPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(millis(200), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, unknownPod()))
            emit(
                millis(300),
                ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected)
            ) // delay of 500ms
            emit(millis(200), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, successfulPod()))
        }

        // EXERCISE
        whenever(api.getLogs(any())).thenReturn("LOGS")
        val result = nextTerminatedSnapshot(stream, millis(600), millis(1000), millis(1000))

        // VERIFY
        StepVerifier.create(result)
            .verifyError<PodNotRunningTimeoutException> {
                assertEquals(ExecutionSnapshot(Logs("LOGS"), InitialJobSnapshot, expected), it.currentState)
            }
        verify(api).getLogs(expected.reference())
    }

    @Test
    fun runningTerminatedDelayed_TerminatedTimeout() {
        // SETUP
        val expected = runningPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(millis(200), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, unknownPod()))
            emit(millis(300), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected)) // 500 ms
            emit(millis(300), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, successfulPod()))//800 ms
        }

        // EXERCISE
        whenever(api.getLogs(any())).thenReturn("LOGS")
        val result = nextTerminatedSnapshot(
            stream,
            millis(700), // running before 700ms
            millis(750),  // terminated before 750ms
            millis(1000),
        )

        // VERIFY
        StepVerifier.create(result)
            .verifyError<PodNotTerminatedTimeoutException> {
                assertEquals(ExecutionSnapshot(Logs("LOGS"), InitialJobSnapshot, expected), it.currentState)
            }
        verify(api).getLogs(expected.reference())
    }


    @Test
    fun terminatedWithErrorAndLogs() {
        // SETUP
        val expected = failedPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(millis(100), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, waitingPod()))
            emit(millis(100), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, runningPod()))
            emit(millis(100), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected)) // 300 ms
        }

        // EXERCISE
        whenever(api.getLogs(any())).thenReturn("LOGS")
        val result = nextTerminatedSnapshot(stream, millis(500), millis(500), millis(500))

        // VERIFY
        StepVerifier.create(result)
            .verifyError<PodTerminatedWithErrorException> {
                assertEquals(ExecutionSnapshot(Logs("LOGS"), InitialJobSnapshot, expected), it.currentState)
            }
        verify(api).getLogs(expected.reference())
    }

    @Test
    fun terminatedWithErrorAndNoLogs() {
        // SETUP
        val expected = failedPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(millis(100), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, waitingPod()))
            emit(millis(100), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, runningPod()))
            emit(millis(100), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected)) // 300 ms
        }

        // EXERCISE
        whenever(api.getLogs(any())).thenReturn(null)
        val result = nextTerminatedSnapshot(stream, millis(500), millis(500), millis(500))

        // VERIFY
        StepVerifier.create(result)
            .verifyError<PodTerminatedWithErrorException> {
                assertEquals(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected), it.currentState)
            }
        verify(api).getLogs(expected.reference())
    }

    @Test
    fun terminatedDelayed_OuterTimeout() {
        // SETUP
        val expected = successfulPod()
        val stream: Flux<ExecutionSnapshot> = cachedEmitter(1) {
            emit(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            emit(millis(100), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, waitingPod()))
            emit(millis(100), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, runningPod()))
            emit(millis(100), ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expected)) // 300 ms
        }

        // EXERCISE
        val result = nextTerminatedSnapshot(stream, millis(500), millis(500), millis(200))

        // VERIFY
        StepVerifier.create(result)
            .verifyError<TimeoutException> {}

    }

}