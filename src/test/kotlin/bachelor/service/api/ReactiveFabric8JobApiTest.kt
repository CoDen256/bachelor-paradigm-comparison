package bachelor.service.api

import bachelor.kubernetes.utils.*
import bachelor.reactive.kubernetes.events.Action
import bachelor.service.api.resources.JobReference
import bachelor.service.api.resources.PodReference
import bachelor.service.utils.BaseJobTemplateFiller
import bachelor.service.utils.JobTemplateFileLoader
import com.google.common.truth.Truth.assertThat
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.awaitility.Awaitility
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertContains
import kotlin.test.assertEquals

class ReactiveFabric8JobApiTest {

    private val client = KubernetesClientBuilder()
        .withConfig(ConfigBuilder().build()).build()

    private val resolver = BaseJobTemplateFiller()
    private val jobSpecFile = "/template/job.yaml"
    private val jobSpecProvider =
        JobTemplateFileLoader(File(ReactiveFabric8JobApiTest::class.java.getResource(jobSpecFile)!!.toURI()))

    private val namespace = "client-test"
    private val JOB_CREATED_TIMEOUT = 5L
    private val JOB_DELETED_TIMEOUT = 5L
    private val JOB_DONE_TIMEOUT = 20L

    private val POD_CREATED_TIMEOUT = 5L
    private val POD_READY_TIMEOUT = 5L
    private val POD_TERMINATED_TIMEOUT = 10L
    private val POD_DELETED_TIMEOUT = 15L


    private lateinit var api: ReactiveJobApi

    @BeforeEach
    fun setup(){
        api = ReactiveFabric8JobApi(client, namespace)
        api.deleteAllJobsAndAwaitNoJobsPresent()
        api.startListeners()
    }

    @AfterEach
    fun teardown(){
        api.deleteAllJobsAndAwaitNoJobsPresent()
        api.awaitNoMoreThanOnePodIsPresent()
        api.close()
        api.jobEvents().collectList().block()?.forEach { println(it) }
    }

    @Test
    fun create() {
        val job = api.createAndAwaitUntilJobCreated(0, 0)

        awaitUntilPodCreated(job)

        // wait until is done and verify no job available after ttl = 0 s + execution time = 1 s
        awaitUntilJobDoesNotExist(job)
    }


    @Test
    fun delete() {
        val job = api.createAndAwaitUntilJobCreated(30, 0)
        api.deleteAndAwaitUntilJobDeleted(job)
    }


    @Test
    fun getLogsContains() {
        val job = api.createAndAwaitUntilJobCreated(10, 20)
        val pod = awaitUntilPodCreated(job)

        awaitUntilPodReady(job)

        val logs = api.getLogs(pod).block()!!
        assertContains(logs, "start\n")
    }

    @Test
    fun getLogs() {
        val job = api.createAndAwaitUntilJobCreated(0, 20)
        val pod = awaitUntilPodCreated(job)

        awaitUntilPodTerminated(job)

        val logs = api.getLogs(pod).block()!!
        assertEquals(logs, "start\nslept 0\nend\n")
    }


    @Test
    fun jobEvents_Create() {
        // execute
        api.createAndAwaitUntilJobCreated(10, 0)
        api.stopListeners() // emits complete

        // verify
        val events = api.jobEvents().collectList().block()?.onEach { println(it) }
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
            )
        )
    }

    @Test
    fun jobEvents_CreateDelete() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(10, 0)
        api.deleteAndAwaitUntilJobDeleted(job)

        api.stopListeners() // emits complete

        // verify
        val events = api.jobEvents().collectList().block()?.onEach { println(it) }
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                del(1, 0, null, null),
            )
        )
    }

    @Test
    fun jobEvents_CreateAwaitUntilPodCreatedDelete() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(10, 0)
        awaitUntilPodCreated(job)
        api.deleteAndAwaitUntilJobDeleted(job)

        api.stopListeners() // emits complete

        // verify
        val events = api.jobEvents().collectList().block()?.onEach { println(it) }
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                del(1, 0, null, null),
            )
        )
    }

    @Test
    fun jobEvents_CreateAwaitUntilPodReadyDelete() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(10, 0)
        awaitUntilPodCreated(job)
        awaitUntilPodReady(job)
        api.deleteAndAwaitUntilJobDeleted(job)

        api.stopListeners() // emits complete

        // verify
        val events = api.jobEvents().collectList().block()?.onEach { println(it) }
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                del(1, 0, null, null),
            )
        )
    }

    @Test
    fun jobEvents_CreateAwaitUntilPodTerminatedDelete() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(0, 0)
        awaitUntilPodCreated(job)
        awaitUntilPodReady(job)
        awaitUntilPodTerminated(job)
        api.deleteAndAwaitUntilJobDeleted(job)

        api.stopListeners() // emits complete

        // verify
        val events = api.jobEvents().collectList().block()?.onEach { println(it) }
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                del(1, 0, null, null),
            )
        )
    }


    @Test
    fun jobEvents_CreateAwaitUntilJobDoneAndRemoved() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(0, 0)

        awaitUntilJobDoesNotExist(job) // wait until job is done and deleted

        api.stopListeners() // emits complete

        // verify
        val events = api.jobEvents().collectList().block()?.onEach { println(it) }
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
    }

    @Test
    fun jobEvents_CreateAwaitUntilJobFailedAndRemoved() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(0, 0, -1)

        awaitUntilJobDoesNotExist(job) // wait until job is done and deleted

        api.stopListeners() // emits complete

        // verify
        val events = api.jobEvents().collectList().block()?.onEach { println(it) }
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
    }


    @Test
    fun jobEvents_CreateAwaitUntilJobDidNotStartAndRemoved() {
        // execute
        val job = api.createAndAwaitUntilJobCreated(0, 0, fail = true)

        awaitUntilJobDoesNotExist(job) // wait until job is done and deleted

        api.stopListeners() // emits complete

        // verify
        val events = api.jobEvents().collectList().block()?.onEach { println(it) }
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
    }

    private fun awaitUntilJobDoesNotExist(job: JobReference, timeout: Duration = Duration.ofSeconds(JOB_DONE_TIMEOUT)) {
        Awaitility.await()
            .atMost(timeout)
            .until { !jobExists(job) }
    }


    private fun ReactiveJobApi.createAndAwaitUntilJobCreated(executionTime: Long, ttl: Long, exitCode: Int = 0, fail: Boolean = false): JobReference {
        val job = create(resolveSpec(executionTime, ttl, exitCode, fail)).block()!!
        Awaitility.await()
            .atMost(Duration.ofSeconds(JOB_CREATED_TIMEOUT))
            .until { jobExists(job) }
        return job
    }

    private fun awaitUntilPodCreated(job: JobReference): PodReference {
        val pod = AtomicReference<PodReference>()
        Awaitility.await()
            .atMost(Duration.ofSeconds(POD_CREATED_TIMEOUT))
            .until { findPod(job)?.let { pod.set(it) } != null }
        return pod.get()
    }

    private fun JobReference.awaitUntilMatches(timeout: Long, active: Int?, ready: Int?, failed: Int?, succeeded: Int?) {
        Awaitility.await()
            .atMost(Duration.ofSeconds(timeout))
            .until { jobExists(this, active, ready, failed, succeeded) }
    }

    private fun awaitUntilPodReady(job: JobReference) {
        Awaitility.await()
            .atMost(Duration.ofSeconds(POD_READY_TIMEOUT))
            .until { podIsReady(findPod(job)!!) }
    }

    private fun awaitUntilPodTerminated(job: JobReference) {
        Awaitility.await()
            .atMost(Duration.ofSeconds(POD_TERMINATED_TIMEOUT))
            .until { podIsTerminated(findPod(job)!!) }
    }

    private fun podIsTerminated(ref: PodReference): Boolean {
        val pod = client.pods().inNamespace(ref.namespace).withName(ref.name).get()
        return pod.status.containerStatuses[0].state.terminated != null
    }

    private fun podIsReady(ref: PodReference): Boolean {
        val pod = client.pods().inNamespace(ref.namespace).withName(ref.name).get()
        return pod.status.containerStatuses[0].state.let {
            it.running != null || it.terminated != null
        }
    }

    private fun ReactiveJobApi.deleteAndAwaitUntilJobDeleted(job: JobReference) {
        delete(job)
        Awaitility.await()
            .atMost(Duration.ofSeconds(JOB_DELETED_TIMEOUT))
            .until { !jobExists(job) }
    }
    private fun ReactiveJobApi.deleteAllJobsAndAwaitNoJobsPresent(){
        getJobs().forEach {
            deleteAndAwaitUntilJobDeleted(it)
        }
    }

    private fun ReactiveJobApi.awaitNoMoreThanOnePodIsPresent(){
        Awaitility.await()
            .atMost(Duration.ofSeconds(POD_DELETED_TIMEOUT))
            .until { getPods().size <= 1 }
    }


    private fun jobExists(job: JobReference, active: Int?, ready: Int?, failed: Int?, succeeded: Int?) = getJobs().any {
        getJobs().any { (name, uid) ->
            job.matches(name, uid) && job.matches(active, ready, failed, succeeded)
        }
    }

    private fun jobExists(job: JobReference) = getJobs().any { (name, uid) ->
        job.matches(name, uid)
    }

    private fun JobReference.matches(name: String, uid: String) =
        name == TARGET_JOB && name == this.name && uid == this.uid

    private fun JobReference.matches(active: Int?, ready: Int?, failed: Int?, succeeded: Int?): Boolean {
        val job = client.batch().v1().jobs().inNamespace(namespace).withName(name).get()
        println("FOUND: ${job.snapshot(Action.UPDATE)}")
        return job.status.let {
            it.active == active && it.ready == ready && it.succeeded == succeeded && it.failed == failed
        }
    }

    private fun getJobs(): List<JobReference> {
        return client.batch().v1().jobs().inNamespace(namespace)
            .list()
            .items
            .map { JobReference(it.metadata.name, it.metadata.uid, it.metadata.namespace) }
    }

    private fun findPod(job: JobReference): PodReference? {
        return getPods().find { it.jobId == job.uid }
    }


    private fun getPods(): List<PodReference> {
        return client.pods().inNamespace(namespace)
            .list()
            .items
            .map { it.reference() }
    }

    private fun resolveSpec(executionTime: Long, ttl: Long, exitCode: Int = 0, fail: Boolean = false): String {
        return resolver.fill(
            jobSpecProvider.getTemplate(), mapOf(
                "NAME" to TARGET_JOB,
                "SLEEP" to "$executionTime",
                "TTL" to "$ttl",
                "CODE" to "$exitCode"
            )
        )
    }

    private inline fun <reified T : Any> List<T>.log(): List<T> {
        if (isEmpty()) {
            println("${T::class.java.simpleName}(<empty>)")
        }
        return map {
            println(it)
            it
        }
    }
}
