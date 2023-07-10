package bachelor.executor.reactive

import bachelor.cachedEmitter
import bachelor.core.api.JobApi
import bachelor.core.api.snapshot.*
import bachelor.core.executor.*
import bachelor.core.impl.template.*
import bachelor.core.utils.*
import bachelor.core.utils.generate.*
import bachelor.executor.reactive.Action.*
import bachelor.millis
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Flux
import reactor.test.StepVerifier

/** Tests pertaining [ReactiveJobExecutor.filterAndCombineSnapshots] method */
@ExtendWith(MockitoExtension::class)
class ReactiveJobExecutorCombineTest {

    @Mock
    lateinit var api: JobApi

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
            emit(millis(200), expectedJob)
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
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob, InitialPodSnapshot))
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
            emit(expectedPod)
            // delay is not needed, because jobStream gets subscribed first, so there is always
            // at least an unknown job before the incoming two pod states
            completeOnLast()
        }

        // EXERCISE
        val result = combineSnapshots(podStream, jobStream)

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, expectedPod))
            .verifyComplete()
    }

    @Test
    fun combinePodAndJobEvent() {
        // SETUP
        val expectedJob = inactiveJob()
        val expectedPod = waitingPod()

        val jobStream: Flux<ActiveJobSnapshot> = cachedEmitter {
            emit(expectedJob)
            completeOnLast()
        }
        val podStream: Flux<ActivePodSnapshot> = cachedEmitter {
            emit(expectedPod)
            completeOnLast()
        }

        // because there is no delay and jobStream gets subscribed first
        // initial stream: "initJob-inactiveJob-initPod-waitingPod"
        // resulting in "inactiveJob+initPod -> waitingPod+inactiveJob"

        // EXERCISE
        val result = combineSnapshots(podStream, jobStream)

        // VERIFY
        StepVerifier.create(result)
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob, InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob, expectedPod))
            .verifyComplete()
    }

    @Test
    fun combinePodAndJobEventWithDelays() {
        // SETUP
        val expectedJob = inactiveJob()
        val expectedPod = waitingPod()
        val jobStream: Flux<ActiveJobSnapshot> = cachedEmitter {
            emit(millis(200), expectedJob)
            completeOnLast()
        }
        val podStream: Flux<ActivePodSnapshot> = cachedEmitter {
            emit(millis(250), expectedPod)
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
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob, InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob, expectedPod))
            .verifyComplete()
    }

    @Test
    fun ignoreIrrelevantEvents() {
        // SETUP
        val expectedJob = inactiveJob()
        val expectedPod = waitingPod()
        val jobStream: Flux<ActiveJobSnapshot> = cachedEmitter {
            emit(inactiveJob("wrongJob"))
            emit(millis(100), inactiveJob("wrongJob"))
            emit(millis(100), inactiveJob("wrongJob"))
            emit(millis(100), expectedJob) // actual job
            emit(millis(100), inactiveJob("wrongJob"))
            completeOnLast()
        }
        val podStream: Flux<ActivePodSnapshot> = cachedEmitter {
            emit(unknownPod("wrongPod", "wrongJob"))
            emit(millis(100), waitingPod("wrongPod", "wrongJob"))
            emit(millis(100), runningPod("wrongPod", "wrongJob"))
            emit(millis(200), expectedPod) // actual pod
            emit(millis(100), failedPod("wrongPod", "wrongJob"))
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
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob, InitialPodSnapshot))
            .expectNext(ExecutionSnapshot(Logs.empty(), expectedJob, expectedPod))
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
        val inactiveJob = inactiveJob()
        val activeJob = activeJob()
        val runningJob1 = runningJob()
        val runningJob2 = runningJob()
        val terminatedJob = succeededJob()

        val waitingPod = waitingPod()
        val runningPod = runningPod()
        val terminatedPod = successfulPod()

        val interval = millis(200)
        val podDelay = millis(100)

        val jobStream: Flux<ActiveJobSnapshot> = cachedEmitter {
            emit(runningJob("wrongJob"))
            emit(interval, inactiveJob, runningJob("wrongJob"))
            emit(interval, activeJob, succeededJob("wrongJob"))
            emit(interval, runningJob1)
            emit(interval, runningJob2, activeJob("wrongJob"))
            emit(interval, terminatedJob)
            completeOnLast()
        }
        val podStream: Flux<ActivePodSnapshot> = cachedEmitter {
            emit(podDelay, runningPod("wrongPod", "wrongJob"))
            emit(interval, runningPod("wrongPod", "wrongJob"))
            emit(interval, waitingPod, successfulPod("wrongPod", "wrongJob"))
            emit(interval, runningPod)
            emit(interval, terminatedPod, waitingPod("wrongPod", "wrongJob"))
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