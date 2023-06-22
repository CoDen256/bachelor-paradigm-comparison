package bachelor.kubernetes.executor

import bachelor.kubernetes.utils.*
import bachelor.reactive.kubernetes.ReactiveJobExecutor
import bachelor.service.api.ReactiveJobApi
import bachelor.reactive.kubernetes.events.Action.*
import bachelor.service.config.fabric8.snapshot
import bachelor.service.api.snapshot.*
import bachelor.service.executor.*
import io.fabric8.kubernetes.api.model.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Flux
import reactor.test.StepVerifier

/**
 * Tests pertaining [KubernetesJobExecutor.filterAndCombineSnapshots]
 * method
 */
@ExtendWith(MockitoExtension::class)
class KubernetesJobExecutorCombineTest {

    @Mock
    lateinit var api: ReactiveJobApi

    private fun combineSnapshots(
        podStream: Flux<ActivePodSnapshot>,
        jobStream: Flux<ActiveJobSnapshot>
    ): Flux<ExecutionSnapshot> {
        // trigger the subscription so elements without delay will be emitted and cached
        podStream.log("ALL PODS").subscribe()
        jobStream.log("ALL JOBS").subscribe()
        return ReactiveJobExecutor(api)
            .filterAndCombineSnapshots(podStream, jobStream, TARGET_JOB)
    }

    @Test
    fun combineEmptyStreams() {
        // SETUP
        // The Flux is cached, so the events emitted immediately will be replayed for late subscribers
        val jobStream: Flux<ActiveJobSnapshot> = cachedEmitter {
            completeOnLast()
        }
        val podStream: Flux<ActivePodSnapshot> = cachedEmitter {
            completeOnLast() // in the actual stream there will be no completion, it just makes it easier to test
        }

        // EXERCISE
        val result = combineSnapshots(podStream, jobStream)

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            .verifyComplete()
    }

    @Test
    fun combineOnlyJobEvent() {
        // SETUP
        val expectedJob = inactiveJob()
        val jobStream: Flux<ActiveJobSnapshot> = cachedEmitter {
            emit(millis(200), expectedJob.snapshot())
            // jobStream gets subscribed first, so if there is two job states before the pod state
            // it will result into one global state with the latest states
            // we need a little delay, so two jobs will be separately emitted
            completeOnLast()
        }
        val podStream: Flux<ActivePodSnapshot> = cachedEmitter {
            completeOnLast()
        }

        // EXERCISE
        val result = combineSnapshots(podStream, jobStream)

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob.snapshot(), InitialPodSnapshot))
            .verifyComplete()
    }

    @Test
    fun combineOnlyPodEvent() {
        // SETUP
        val expectedPod = waitingPod()
        val jobStream: Flux<ActiveJobSnapshot> = cachedEmitter {
            completeOnLast()
        }
        val podStream: Flux<ActivePodSnapshot> = cachedEmitter {
            emit(expectedPod.snapshot())
            // delay is not needed, because jobStream gets subscribed first, so there is always
            // at least an unknown job before the incoming two pod states
            completeOnLast()
        }

        // EXERCISE
        val result = combineSnapshots(podStream, jobStream)

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expectedPod.snapshot()))
            .verifyComplete()
    }

    @Test
    fun combinePodAndJobEvent() {
        // SETUP
        val expectedJob = inactiveJob()
        val expectedPod = waitingPod()

        val jobStream: Flux<ActiveJobSnapshot> = cachedEmitter {
            emit(expectedJob.snapshot())
            completeOnLast()
        }
        val podStream: Flux<ActivePodSnapshot> = cachedEmitter {
            emit(expectedPod.snapshot())
            completeOnLast()
        }

        // because there is no delay and jobStream gets subscribed first
        // initial stream: "initJob-inactiveJob-initPod-waitingPod"
        // resulting in "inactiveJob+initPod -> waitingPod+inactiveJob"

        // EXERCISE
        val result = combineSnapshots(podStream, jobStream)

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob.snapshot(), InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob.snapshot(), expectedPod.snapshot()))
            .verifyComplete()
    }

    @Test
    fun combinePodAndJobEventWithDelays() {
        // SETUP
        val expectedJob = inactiveJob()
        val expectedPod = waitingPod()
        val jobStream: Flux<ActiveJobSnapshot> = cachedEmitter {
            emit(millis(200), expectedJob.snapshot())
            completeOnLast()
        }
        val podStream: Flux<ActivePodSnapshot> = cachedEmitter {
            emit(millis(250), expectedPod.snapshot())
            completeOnLast()
        }

        // because there is a delay and the jobStream gets subscribed first
        // the initial stream would be "initJob-initPod-inactiveJob-waitingPod"
        // resulting in "initJob+initPod -> inactiveJob+initPod -> waitingPod+inactiveJob"

        // EXERCISE
        val result = combineSnapshots(
            podStream, jobStream
        )

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob.snapshot(), InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob.snapshot(), expectedPod.snapshot()))
            .verifyComplete()
    }

    @Test
    fun ignoreIrrelevantEvents() {
        // SETUP
        val expectedJob = inactiveJob()
        val expectedPod = waitingPod()
        val jobStream: Flux<ActiveJobSnapshot> = cachedEmitter {
            emit(inactiveJobSnapshot("wrongJob"))
            emit(millis(100), inactiveJobSnapshot("wrongJob"))
            emit(millis(100), inactiveJobSnapshot("wrongJob"))
            emit(millis(100), expectedJob.snapshot()) // actual job
            emit(millis(100), inactiveJobSnapshot("wrongJob"))
            completeOnLast()
        }
        val podStream: Flux<ActivePodSnapshot> = cachedEmitter {
            emit(unknownSnapshot("wrongPod", "wrongJob"))
            emit(millis(100), waitingSnapshot("wrongPod", "wrongJob"))
            emit(millis(100), runningSnapshot("wrongPod", "wrongJob"))
            emit(millis(200), expectedPod.snapshot()) // actual pod
            emit(millis(100), failedSnapshot("wrongPod", "wrongJob"))
            completeOnLast()
        }

        // because there is a delay and jobStream gets subscribed first
        // initial stream: "initJob-initPod-inactiveJob-waitingPod"
        // resulting in "initPod+initPod -> inactiveJob+initPod -> waitingPod+inactiveJob"
        // EXERCISE
        val result = combineSnapshots(podStream, jobStream).log("COMBINED")

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob.snapshot(), InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob.snapshot(), expectedPod.snapshot()))
            .verifyComplete()
    }


    /**
     * - Time : |000ms-----|200ms-----|400ms-----|600ms-----|800ms-----|1000ms
     * - Actual Jobs :
     *   |Unknown---|Empty-----|Active----|Running---|Running---|Terminated
     * - Fake Jobs : |Running---|Running---|Terminated|----------|Active----|
     * - Pod Time :
     *   |100ms-----|300ms-----|500ms-----|700ms-----|900ms-----|1100ms
     * - Actual Pods : |Unknown---|----------|Waiting---|Running---|Terminated|
     * - Fake Pods : |Running---|Running---|Terminated|----------|Waiting---|
     */
    @Test
    fun complexEventGeneration() {
        // SETUP
        val inactiveJob = inactiveJobSnapshot()
        val activeJob = activeJobSnapshot()
        val runningJob1 = runningJobSnapshot()
        val runningJob2 = runningJobSnapshot()
        val terminatedJob = succeededJobSnapshot()

        val waitingPod = waitingSnapshot()
        val runningPod = runningSnapshot()
        val terminatedPod = successfulSnapshot()

        val interval = millis(200)
        val podDelay = millis(100)

        val jobStream: Flux<ActiveJobSnapshot> = cachedEmitter {
            emit(runningJobSnapshot("wrongJob"))
            emit(interval, inactiveJob, runningJobSnapshot("wrongJob"))
            emit(interval, activeJob, succeededJobSnapshot("wrongJob"))
            emit(interval, runningJob1)
            emit(interval, runningJob2, activeJobSnapshot("wrongJob"))
            emit(interval, terminatedJob)
            completeOnLast()
        }
        val podStream: Flux<ActivePodSnapshot> = cachedEmitter {
            emit(podDelay, runningSnapshot("wrongPod", "wrongJob"))
            emit(interval, runningSnapshot("wrongPod", "wrongJob"))
            emit(interval, waitingPod, successfulSnapshot("wrongPod", "wrongJob"))
            emit(interval, runningPod)
            emit(interval, terminatedPod, waitingSnapshot("wrongPod", "wrongJob"))
            completeOnLast()
        }

        // EXERCISE
        val result = combineSnapshots(podStream, jobStream).log("COMBINED")

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), inactiveJob, InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), activeJob, InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), activeJob, waitingPod))
            .expectNext(ExecutionSnapshot(Logs.empty(), runningJob1, waitingPod))
            .expectNext(ExecutionSnapshot(Logs.empty(), runningJob1, runningPod))
            .expectNext(ExecutionSnapshot(Logs.empty(), runningJob2, runningPod))
            .expectNext(ExecutionSnapshot(Logs.empty(), runningJob2, terminatedPod))
            .expectNext(ExecutionSnapshot(Logs.empty(), terminatedJob, terminatedPod))
            .verifyComplete()
    }


}