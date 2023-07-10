package bachelor.core.impl.api

import bachelor.*
import bachelor.core.api.JobApi
import bachelor.core.api.ResourceEventListener
import bachelor.core.api.snapshot.*
import bachelor.core.api.snapshot.Phase.*
import bachelor.core.utils.generate.*
import bachelor.executor.reactive.ResourceEvent
import com.google.common.truth.Truth.assertThat
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertContains
import kotlin.test.assertEquals

abstract class AbstractJobApiIT(
    private val createJobApi: (String) -> JobApi
) {
    private val helper = createHelperClient()
    private lateinit var api: JobApi

    private val jobEvents = ArrayList<ResourceEvent<ActiveJobSnapshot>>()
    private val podEvents = ArrayList<ResourceEvent<ActivePodSnapshot>>()

    private val podListener = ResourceEventListener { podEvents.add(it) }
    private val jobListener = ResourceEventListener { jobEvents.add(it) }

    @BeforeEach
    fun setup() {
        jobEvents.clear()
        podEvents.clear()
        api = createJobApi(NAMESPACE)
        api.deleteAllJobsAndAwaitNoJobsPresent()
        api.startListeners()
        api.addPodListener(podListener)
        api.addJobListener(jobListener)
    }

    @AfterEach
    fun teardown() {
        api.deleteAllJobsAndAwaitNoJobsPresent()
        helper.awaitNoPodsPresent(NAMESPACE)
        api.removePodListener(podListener)
        api.removeJobListener(jobListener)
        api.close() // stopListeners
        collectJobEvents()
        collectPodEvents()
    }

    @Test
    fun create() {
        api = createJobApi(NAMESPACE)
        val job = api.createAndAwaitUntilJobCreated(0, 0)

        helper.awaitUntilPodCreated(job, NAMESPACE)

        // wait until is done and verify no job available after ttl = 0 s + execution time = 1 s
        helper.awaitUntilJobDoesNotExist(job, NAMESPACE)
    }


    @Test
    fun delete() {
        val job = api.createAndAwaitUntilJobCreated(2, 0)
        api.deleteAndAwaitUntilJobDeleted(job)
    }


    @Test
    fun getLogsContains() {
        val job = api.createAndAwaitUntilJobCreated(2, 20)
        val pod = helper.awaitUntilPodCreated(job, NAMESPACE)

        helper.awaitUntilPodReady(job, NAMESPACE)

        val logs = api.collectLogs(pod)
        assertContains(logs, "start\n")
    }

    @Test
    fun getLogs() {
        val job = api.createAndAwaitUntilJobCreated(0, 20)
        val pod = helper.awaitUntilPodCreated(job, NAMESPACE)

        helper.awaitUntilPodTerminated(job, NAMESPACE)

        val logs = api.collectLogs(pod)
        assertEquals(logs, "start\nslept 0\nend\n")
    }


    @Test
    fun eventsListenerStartedTwice() {
        assertThrows<IllegalStateException> { api.startListeners() }
    }

    @Test
    fun events_HighExecutionTime_Create() {
        // execute
        api.createAndAwaitUntilJobCreated(1, 0) // execution time 1, waiting will NOT be skipped
        api.stopListeners() // emits complete

        // verify
        val events = collectJobEvents()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
            )
        )
        val podEvents = collectPodEvents()
        val name = podEvents[0].element!!.name
        assertThat(podEvents).containsExactlyElementsIn(
            listOf(
                add(PENDING, name = name),
                upd(PENDING, name = name),
                upd(PENDING, waiting("ContainerCreating"), name = name),
            )
        )
    }

    @Test
    fun events_HighExecutionTime_CreateDelete() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(1, 0) // execution time 1, waiting will NOT be skipped
        api.deleteAndAwaitUntilJobDeleted(job)

        api.stopListeners() // emits complete

        // verify
        val events = collectJobEvents()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                del(1, 0, null, null),
            )
        )
        val podEvents = collectPodEvents()
        val name = podEvents[0].element!!.name
        assertThat(podEvents).containsExactlyElementsIn(
            listOf(
                add(PENDING, name = name),
                upd(PENDING, name = name),
                upd(PENDING, waiting("ContainerCreating"), name = name),
                upd(PENDING, waiting("ContainerCreating"), name = name),
                upd(PENDING, waiting("ContainerCreating"), name = name),
            )
        )
    }

    @Test
    fun events_HighExecutionTime_CreateAwaitUntilPodCreatedDelete() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(1, 0) // execution time 1, waiting will NOT be skipped
        val pod = helper.awaitUntilPodCreated(job, NAMESPACE)
        api.deleteAndAwaitUntilJobDeleted(job)

        api.stopListeners() // emits complete

        // verify
        val events = collectJobEvents()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                del(1, 0, null, null),
            )
        )
        val podEvents = collectPodEvents()
        assertThat(podEvents).containsExactlyElementsIn(
            listOf(
                add(PENDING, name = pod.name),
                upd(PENDING, name = pod.name),
                upd(PENDING, waiting("ContainerCreating"), name = pod.name),
                upd(PENDING, waiting("ContainerCreating"), name = pod.name),
                upd(PENDING, waiting("ContainerCreating"), name = pod.name),
            )
        )
    }

    @Test
    fun events_HighExecutionTime_CreateAwaitUntilPodReadyDelete() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(2, 0) // execution time 2, running will be present
        val pod = helper.awaitUntilPodCreated(job, NAMESPACE)
        helper.awaitUntilPodReady(job, NAMESPACE)
        api.deleteAndAwaitUntilJobDeleted(job)

        api.stopListeners() // emits complete

        // verify
        val events = collectJobEvents()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                del(1, 0, null, null),
            )
        )
        val podEvents = collectPodEvents()
        assertThat(podEvents).containsExactlyElementsIn(
            listOf(
                add(PENDING, name = pod.name),
                upd(PENDING, name = pod.name),
                upd(PENDING, waiting("ContainerCreating"), name = pod.name),
                upd(RUNNING, running(podEvents[3].getRunningStartedAt()), name = pod.name),
                upd(RUNNING, running(podEvents[4].getRunningStartedAt()), name = pod.name),
                upd(RUNNING, running(podEvents[5].getRunningStartedAt()), name = pod.name),
            )
        )
    }

    @Test
    fun events_HighExecutionTime_CreateAwaitUntilPodTerminatedDelete() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(2, 0) // execution time 2, running will NOT be skipped
        val pod = helper.awaitUntilPodCreated(job, NAMESPACE)
        helper.awaitUntilPodReady(job, NAMESPACE)
        helper.awaitUntilPodTerminated(job, NAMESPACE)
        api.deleteAndAwaitUntilJobDeleted(job)

        api.stopListeners() // emits complete

        // verify
        val events = collectJobEvents()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                upd(1, 1, null, null),
                del(1, 1, null, null),
            )
        )

        val podEvents = collectPodEvents()
        assertThat(podEvents).containsExactlyElementsIn(
            listOf(
                add(PENDING, name = pod.name),
                upd(PENDING, name = pod.name),
                upd(PENDING, waiting("ContainerCreating"), name = pod.name),
                upd(RUNNING, running(podEvents[3].getRunningStartedAt()), name = pod.name),
                upd(RUNNING, terminated(0, "Completed"), name = pod.name),
                upd(RUNNING, terminated(0, "Completed"), name = pod.name),
                upd(RUNNING, terminated(0, "Completed"), name = pod.name),
            )
        )
    }

    @Test
    fun events_CreateAwaitUntilPodTerminatedDelete() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(0, 0) // execution time 0, running will be skipped
        val pod = helper.awaitUntilPodCreated(job, NAMESPACE)
        helper.awaitUntilPodReady(job, NAMESPACE)
        helper.awaitUntilPodTerminated(job, NAMESPACE)
        api.deleteAndAwaitUntilJobDeleted(job)

        api.stopListeners() // emits complete

        // verify
        val events = collectJobEvents()
        assertThat(events).containsAtLeastElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                del(1, 0, null, null),
            )
        )

        val podEvents = collectPodEvents()
        assertThat(podEvents).containsExactlyElementsIn(
            listOf(
                add(PENDING, name = pod.name),
                upd(PENDING, name = pod.name),
                upd(PENDING, waiting("ContainerCreating"), name = pod.name),
                upd(PENDING, terminated(0, "Completed"), name = pod.name),
                upd(PENDING, terminated(0, "Completed"), name = pod.name),
                upd(PENDING, terminated(0, "Completed"), name = pod.name),
            )
        )
    }

    @Test
    fun events_CreateAwaitUntilJobDoneAndRemoved() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(0, 0) // execution time 0, running will be skipped
        val pod = helper.awaitUntilPodCreated(job, NAMESPACE)

        helper.awaitUntilJobDoesNotExist(job, NAMESPACE) // wait until job is done and deleted

        api.stopListeners() // emits complete

        // verify
        val events = collectJobEvents()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),

                upd(null, 0, null, null),

                upd(null, 0, null, 1, listOf("Complete")),
                upd(null, 0, null, 1, listOf("Complete")),
                del(null, 0, null, 1, listOf("Complete")),
            )
        )
        val podEvents = collectPodEvents()
        assertThat(podEvents).containsExactlyElementsIn(
            listOf(
                add(PENDING, name = pod.name),
                upd(PENDING, name = pod.name),
                upd(PENDING, waiting("ContainerCreating"), name = pod.name),
                upd(PENDING, terminated(0, "Completed"), name = pod.name),
                upd(SUCCEEDED, terminated(0, "Completed"), name = pod.name),
                upd(SUCCEEDED, terminated(0, "Completed"), name = pod.name),
                upd(SUCCEEDED, terminated(0, "Completed"), name = pod.name),
                del(SUCCEEDED, terminated(0, "Completed"), name = pod.name),
            )
        )
    }

    @Test
    fun events_HighExecutionTime_CreateAwaitUntilJobDoneAndRemoved() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(2, 0) // execution time 2, running will NOT be skipped
        val pod = helper.awaitUntilPodCreated(job, NAMESPACE)

        helper.awaitUntilJobDoesNotExist(job, NAMESPACE) // wait until job is done and deleted

        api.stopListeners() // emits complete

        // verify
        val events = collectJobEvents()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),

                upd(1, 1, null, null), // running
                upd(1, 0, null, null), // running

                upd(null, 0, null, null),

                upd(null, 0, null, 1, listOf("Complete")),
                upd(null, 0, null, 1, listOf("Complete")),
                del(null, 0, null, 1, listOf("Complete")),
            )
        )
        val podEvents = collectPodEvents()
        assertThat(podEvents).containsExactlyElementsIn(
            listOf(
                add(PENDING, name = pod.name),
                upd(PENDING, name = pod.name),
                upd(PENDING, waiting("ContainerCreating"), name = pod.name),

                upd(RUNNING, running(podEvents[3].getRunningStartedAt()), name = pod.name),

                upd(RUNNING, terminated(0, "Completed"), name = pod.name),
                upd(SUCCEEDED, terminated(0, "Completed"), name = pod.name),
                upd(SUCCEEDED, terminated(0, "Completed"), name = pod.name),
                upd(SUCCEEDED, terminated(0, "Completed"), name = pod.name),
                del(SUCCEEDED, terminated(0, "Completed"), name = pod.name),
            )
        )
    }

    @Test
    fun events_CreateAwaitUntilJobFailedAndRemoved() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(0, 0, 3) // execution time 0, running WILL be skipped
        val pod = helper.awaitUntilPodCreated(job, NAMESPACE)

        helper.awaitUntilJobDoesNotExist(job, NAMESPACE) // wait until job is done and deleted

        api.stopListeners() // emits complete

        // verify
        val events = collectJobEvents()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),

                upd(null, 0, null, null),

                upd(null, 0, 1, null, listOf("Failed")),
                upd(null, 0, 1, null, listOf("Failed")),
                del(null, 0, 1, null, listOf("Failed")),
            )
        )

        val podEvents = collectPodEvents()
        assertThat(podEvents).containsExactlyElementsIn(
            listOf(
                add(PENDING, name = pod.name),
                upd(PENDING, name = pod.name),
                upd(PENDING, waiting("ContainerCreating"), name = pod.name),
                upd(PENDING, terminated(3, "Error"), name = pod.name),
                upd(FAILED, terminated(3, "Error"), name = pod.name),
                upd(FAILED, terminated(3, "Error"), name = pod.name),
                upd(FAILED, terminated(3, "Error"), name = pod.name),
                del(FAILED, terminated(3, "Error"), name = pod.name),
            )
        )
    }

    @Test
    fun events_HighExecutionTime_CreateAwaitUntilJobFailedAndRemoved() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(2, 0, 3) // execution time 0, running will NOT be skipped
        val pod = helper.awaitUntilPodCreated(job, NAMESPACE)

        helper.awaitUntilJobDoesNotExist(job, NAMESPACE) // wait until job is done and deleted

        api.stopListeners() // emits complete

        // verify
        val events = collectJobEvents()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),

                upd(1, 1, null, null), // running
                upd(1, 0, null, null), // running

                upd(null, 0, null, null),

                upd(null, 0, 1, null, listOf("Failed")),
                upd(null, 0, 1, null, listOf("Failed")),
                del(null, 0, 1, null, listOf("Failed")),
            )
        )

        val podEvents = collectPodEvents()
        assertThat(podEvents).containsExactlyElementsIn(
            listOf(
                add(PENDING, name = pod.name),
                upd(PENDING, name = pod.name),
                upd(PENDING, waiting("ContainerCreating"), name = pod.name),

                upd(RUNNING, running(podEvents[3].getRunningStartedAt()), name = pod.name),
                upd(RUNNING, terminated(3, "Error"), name = pod.name),

                upd(FAILED, terminated(3, "Error"), name = pod.name),
                upd(FAILED, terminated(3, "Error"), name = pod.name),
                upd(FAILED, terminated(3, "Error"), name = pod.name),
                del(FAILED, terminated(3, "Error"), name = pod.name),
            )
        )
    }


    @Test
    fun events_CreateAwaitUntilJobDidNotStartAndRemoved() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(0, 0, fail = true)
        val pod = helper.awaitUntilPodCreated(job, NAMESPACE)

        Thread.sleep(5000)

        api.stopListeners() // emits complete

        // verify
        val events = collectJobEvents()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
            )
        )

        val podEvents = collectPodEvents()
        assertThat(podEvents).containsExactlyElementsIn(
            listOf(
                add(PENDING, name = pod.name),
                upd(PENDING, name = pod.name),
                upd(PENDING, waiting("ContainerCreating"), name = pod.name),
                upd(PENDING, waiting("ErrImagePull", podEvents[3].getWaitingMessage()), name = pod.name),
            )
        )
    }

    private fun ResourceEvent<ActivePodSnapshot>.getWaitingMessage(): String {
        val state = element?.mainContainerState
        if (state !is WaitingState) return ""
        return state.message
    }

    private fun ResourceEvent<ActivePodSnapshot>.getRunningStartedAt(): String {
        val state = element?.mainContainerState
        if (state !is RunningState) return ""
        return state.startedAt
    }

    // REACTIVE JOB API EXTENSION HELPER METHODS
    private fun JobApi.createAndAwaitUntilJobCreated(
        executionTime: Long,
        ttl: Long,
        exitCode: Int = 0,
        fail: Boolean = false
    ): JobReference {
        val job = create(resolveSpec(executionTime, ttl, exitCode, fail, NAMESPACE))
        helper.awaitUntilJobCreated(job, NAMESPACE)
        return job
    }

    private fun JobApi.collectLogs(pod: PodReference) = getLogs(pod)

    private fun JobApi.deleteAllJobsAndAwaitNoJobsPresent() {
        helper.getJobs(NAMESPACE).forEach { deleteAndAwaitUntilJobDeleted(it) }
    }

    private fun JobApi.deleteAndAwaitUntilJobDeleted(job: JobReference, timeout: Long = JOB_DELETED_TIMEOUT) {
        delete(job)
        helper.awaitUntilJobDoesNotExist(job, NAMESPACE, timeout)
    }

    private fun collectPodEvents(): List<ResourceEvent<ActivePodSnapshot>> =
        podEvents.also { println("------ POD-EVENTS ------") }.onEach { println(it) }

    private fun collectJobEvents(): List<ResourceEvent<ActiveJobSnapshot>> =
        jobEvents.also { println("------ JOB-EVENTS ------") }.onEach { println(it) }



    // HELPER KUBERNETES CLIENT TO VERIFY ACTUAL STATE ON THE KUBERNETES CLUSTER
    private fun createHelperClient(): KubernetesClient {
        return KubernetesClientBuilder().build().apply {
            createNamespace(NAMESPACE)
        }
    }

}