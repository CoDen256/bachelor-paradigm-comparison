package bachelor.service.api

import bachelor.kubernetes.utils.*
import bachelor.service.api.resources.JobReference
import bachelor.service.api.resources.PodReference
import bachelor.service.utils.BaseJobTemplateFiller
import bachelor.service.utils.JobTemplateFileLoader
import com.google.common.truth.Truth.assertThat
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.awaitility.Awaitility
import org.junit.Test
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
    private val POD_CREATED_TIMEOUT = 5L
    private val POD_READY_TIMEOUT = 5L
    private val POD_TERMINATED_TIMEOUT = 10L


    @Test
    fun create() {
        val job = ReactiveFabric8JobApi(client, namespace)
            .createAndVerifyJobCreated(1, 0)

        verifyPodCreated(job)

        // wait until is done and verify no job available after ttl = 0 s + execution time = 1 s
        Awaitility.await()
            .atLeast(Duration.ofSeconds(1))
            .atMost(Duration.ofSeconds(10))
            .until { !jobExists(job) }
    }


    @Test
    fun delete() {
        val api = ReactiveFabric8JobApi(client, namespace)
        val job = api.createAndVerifyJobCreated(30, 0)
        api.deleteAndVerifyJobDeleted(job)
    }


    @Test
    fun getLogsContains() {
        val api = ReactiveFabric8JobApi(client, namespace)
        val job = api.createAndVerifyJobCreated(10, 20)
        val pod = verifyPodCreated(job)
        println(pod.name)

        verifyPodReady(job)

        val logs = api.getLogs(pod).block()!!
        println(logs)
        assertContains(logs, "start\n")

        api.deleteAndVerifyJobDeleted(job)
    }

    @Test
    fun getLogs() {
        val api = ReactiveFabric8JobApi(client, namespace)
        val job = api.createAndVerifyJobCreated(0, 20)
        val pod = verifyPodCreated(job)
        println(pod.name)

        verifyPodTerminated(job)

        val logs = api.getLogs(pod).block()!!
        println(logs)
        assertEquals(logs, "start\nslept 0\nend\n")

        api.deleteAndVerifyJobDeleted(job)
    }


    @Test
    fun jobEventsCreation() {
        val api = ReactiveFabric8JobApi(client, namespace)

        // execute
        api.startListeners()
        val job = api.createAndVerifyJobCreated(0, 0)
        api.stopListeners()

        // verify
        val events = api.jobEvents().collectList().block()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null)
            )
        )

        api.deleteAndVerifyJobDeleted(job)
    }

    @Test
    fun jobEventsCreationDeletion() {
        val api = ReactiveFabric8JobApi(client, namespace)

        // execute
        api.startListeners()

        val job = api.createAndVerifyJobCreated(0, 0)
        api.deleteAndVerifyJobDeleted(job)

        api.stopListeners()

        // verify
        val events = api.jobEvents().collectList().block()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                del(1, 0, null, null),
            )
        )
    }

    @Test
    fun jobEventsCreationWaitingForPodCreationDeletion() {
        val api = ReactiveFabric8JobApi(client, namespace)

        // execute
        api.startListeners()

        val job = api.createAndVerifyJobCreated(0, 0)
        verifyPodCreated(job)
        api.deleteAndVerifyJobDeleted(job)

        api.stopListeners()

        // verify
        val events = api.jobEvents().collectList().block()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                del(1, 0, null, null),
            )
        )
    }

    @Test
    fun jobEventsCreationWaitingForPodReadinessDeletion() {
        val api = ReactiveFabric8JobApi(client, namespace)

        // execute
        api.startListeners()

        val job = api.createAndVerifyJobCreated(0, 0)
        verifyPodCreated(job)
        verifyPodReady(job)
        api.deleteAndVerifyJobDeleted(job)

        api.stopListeners()

        // verify
        val events = api.jobEvents().collectList().block()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                del(1, 0, null, null),
            )
        )
    }

    @Test
    fun jobEventsCreationWaitingForPodTerminationDeletion() {
        val api = ReactiveFabric8JobApi(client, namespace)

        // execute
        api.startListeners()

        val job = api.createAndVerifyJobCreated(0, 0)
        verifyPodCreated(job)
        verifyPodTerminated(job)
        api.deleteAndVerifyJobDeleted(job)

        api.stopListeners()

        // verify
        val events = api.jobEvents().collectList().block()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                del(1, 0, null, null),
            )
        )
    }

    @Test
    fun jobEventsCreationWaitingForPodTerminationAndWaitDeletion() {
        val api = ReactiveFabric8JobApi(client, namespace)

        // execute
        api.startListeners()

        val job = api.createAndVerifyJobCreated(5, 0)
        verifyPodCreated(job)
        verifyPodTerminated(job)
        job.verifyJob(5, 1, 0, null, null)
//        job.verifyJob(5, null, 0, null, null)
        job.verifyJob(5, null, 0, 1, null)
        job.verifyJob(5, null, 0, 1, null)
        api.deleteAndVerifyJobDeleted(job)
        api.stopListeners()

        // verify
        val events = api.jobEvents().collectList().block()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                upd(1, 1, null, null),
                del(1, 1, null, null),
            )
        )
    }

    @Test
    fun jobEventsCreationWaitingForPodTermination5SecondsAndDelay2SecondsDeletion() {
        val api = ReactiveFabric8JobApi(client, namespace)

        // execute
        api.startListeners()

        val job = api.createAndVerifyJobCreated(0, 20)
        verifyPodCreated(job)
        verifyPodTerminated(job)
        Thread.sleep(2000)
        api.deleteAndVerifyJobDeleted(job)

        api.stopListeners()

        // verify
        val events = api.jobEvents().collectList().block()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                upd(1, 1, null, null),
                upd(1, 0, null, null),
                del(1, 0, null, null),
            )
        )
    }

    @Test
    fun jobEventsCreationWaitingForPodTermination5SecondsAndDelay5SecondsDeletion() {
        val api = ReactiveFabric8JobApi(client, namespace)

        // execute
        api.startListeners()

        val job = api.createAndVerifyJobCreated(0, 20)
        verifyPodCreated(job)
        verifyPodTerminated(job)
        Thread.sleep(6000)
        api.deleteAndVerifyJobDeleted(job)

        api.stopListeners()

        // verify
        val events = api.jobEvents().collectList().block()
        assertThat(events).containsExactlyElementsIn(
            listOf(
                add(null, null, null, null),
                upd(1, 0, null, null),
                upd(1, 1, null, null),
                upd(1, 0, null, null),
                upd(null, 0, null, null),
                upd(null, 0, 1, null),
                del(null, 0, 1, null),
            )
        )
    }

    private fun ReactiveFabric8JobApi.createAndVerifyJobCreated(executionTime: Long, ttl: Long): JobReference {
        val job = create(resolveSpec(executionTime, ttl)).block()!!
        Awaitility.await()
            .atMost(Duration.ofSeconds(JOB_CREATED_TIMEOUT))
            .until { jobExists(job) }
        return job
    }

    private fun verifyPodCreated(job: JobReference): PodReference {
        val pod = AtomicReference<PodReference>()
        Awaitility.await()
            .atMost(Duration.ofSeconds(POD_CREATED_TIMEOUT))
            .until { findPod(job)?.let { pod.set(it) } != null }
        return pod.get()
    }

    private fun JobReference.verifyJob(timeout: Long, active: Int?, ready: Int?, succeeded: Int?, failed: Int?) {
        Awaitility.await()
            .atMost(Duration.ofSeconds(timeout))
            .until { jobExists(this, active, ready, succeeded, failed) }
    }

    private fun verifyPodReady(job: JobReference) {
        Awaitility.await()
            .atMost(Duration.ofSeconds(POD_READY_TIMEOUT))
            .until { podIsReady(findPod(job)!!) }
    }

    private fun verifyPodTerminated(job: JobReference) {
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

    private fun ReactiveFabric8JobApi.deleteAndVerifyJobDeleted(job: JobReference) {
        delete(job)
        Awaitility.await()
            .atMost(Duration.ofSeconds(JOB_DELETED_TIMEOUT))
            .until { !jobExists(job) }
    }


    private fun jobExists(job: JobReference, active: Int?, ready: Int?, succeeded: Int?, failed: Int?) = getJobs().any {
        getJobs().any { (name, uid) ->
            job.matches(name, uid) && job.matches(active, ready, succeeded, failed)
        }
    }

    private fun jobExists(job: JobReference) = getJobs().any { (name, uid) ->
        job.matches(name, uid)
    }

    private fun JobReference.matches(name: String, uid: String) =
        name == TARGET_JOB && name == this.name && uid == this.uid

    private fun JobReference.matches(active: Int?, ready: Int?, succeeded: Int?, failed: Int?): Boolean {
        val job = client.batch().v1().jobs().inNamespace(namespace).withName(name).get()
        return job.status.let {
            it.active == active && it.ready == ready && it.succeeded == succeeded && it.failed == failed
        }
    }

    private fun getJobs(): List<JobReference> {
        return client.batch().v1().jobs().inNamespace(namespace)
            .list()
            .items
            .map { JobReference(it.metadata.name, it.metadata.uid, it.metadata.namespace) }
            .log()
    }

    private fun findPod(job: JobReference): PodReference? {
        return getPods().find { it.jobId == job.uid }
    }


    private fun getPods(): List<PodReference> {
        return client.pods().inNamespace(namespace)
            .list()
            .items
            .map { it.reference() }
            .log()
    }

    private fun resolveSpec(executionTime: Long, ttl: Long): String {
        return resolver.fill(
            jobSpecProvider.getTemplate(), mapOf(
                "NAME" to TARGET_JOB,
                "SLEEP" to "$executionTime",
                "TTL" to "$ttl"
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
