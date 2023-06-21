package bachelor.service.api

import bachelor.kubernetes.utils.TARGET_JOB
import bachelor.kubernetes.utils.add
import bachelor.kubernetes.utils.reference
import bachelor.kubernetes.utils.upd
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

        Awaitility.await()
            .atMost(Duration.ofSeconds(POD_READY_TIMEOUT))
            .until { podIsReady(findPod(job)!!) }

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

        Awaitility.await()
            .atMost(Duration.ofSeconds(POD_TERMINATED_TIMEOUT))
            .until { podIsTerminated(findPod(job)!!) }

        val logs = api.getLogs(pod).block()!!
        println(logs)
        assertEquals(logs, "start\nslept 0\nend\n")

        api.deleteAndVerifyJobDeleted(job)
    }

    @Test
    fun jobEventsCreation() {
        val api = ReactiveFabric8JobApi(client, namespace)

        api.startListeners()
        val job = api.createAndVerifyJobCreated(0, 20)
        api.stopListeners()

        val events = api.jobEvents().collectList().block()
        assertThat(events).containsExactlyElementsIn(listOf(
            add(null, null, null, null),
            upd(1, 0, null, null)
        ))

        api.deleteAndVerifyJobDeleted(job)
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


    private fun jobExists(job: JobReference) = getJobs().any { (name, uid) ->
        name == TARGET_JOB && name == job.name && uid == job.uid
    }

    private fun getJobs(): List<JobReference> {
        return client.batch().v1().jobs().inNamespace(namespace)
            .list()
            .items
            .map { JobReference(it.metadata.name, it.metadata.uid, it.metadata.namespace) }
            .log ()
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
        if (isEmpty()){
            println("${T::class.java.simpleName}(<empty>)")
        }
        return map {
            println(it)
            it
        }
    }
}
