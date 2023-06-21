package bachelor.service.api

import bachelor.service.run.ImageRunRequest
import bachelor.service.utils.BaseJobTemplateFiller
import bachelor.service.utils.JobTemplateFileLoader
import io.fabric8.kubernetes.api.model.batch.v1.Job
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.awaitility.Awaitility
import org.junit.Test
import java.io.File
import java.time.Duration

class ReactiveFabric8JobApiTest {

    private val client = KubernetesClientBuilder()
        .withConfig(ConfigBuilder().build()).build()

    private val resolver = BaseJobTemplateFiller()
    private val jobSpecFile = "/template/job.yaml"
    private val jobSpecProvider =
        JobTemplateFileLoader(File(ReactiveFabric8JobApiTest::class.java.getResource(jobSpecFile)!!.toURI()))

    private val namespace = "client-test"

    @Test
    fun create() {
        val job = ReactiveFabric8JobApi(client, namespace)
            .create(resolveSpec(1))
            .block()!!


        Awaitility.await()
            .atMost(Duration.ofSeconds(5))
            .until {
                jobExists(job)
            }

        Awaitility.await()
            .atMost(Duration.ofSeconds(5))
            .until { podExists(job) }

        Awaitility.await()
            .atLeast(Duration.ofSeconds(1))
            .atMost(Duration.ofSeconds(10))
            .until { !jobExists(job) }
    }


    @Test
    fun delete() {
        val api = ReactiveFabric8JobApi(client, namespace)
        val job = api
            .create(resolveSpec(30))
            .block()!!

        Awaitility.await()
            .atMost(Duration.ofSeconds(5))
            .until { jobExists(job) }

        api.delete(job)

        Awaitility.await()
            .atMost(Duration.ofSeconds(5))
            .until { !jobExists(job) }
    }

    private fun podExists(job: Job) = getPods().any { it.second == job.metadata.uid }

    private fun jobExists(job: Job) = getJobs().any { (name, uid) ->
        name == "test-job" &&
                name == job.metadata.name &&
                uid == job.metadata.uid
    }

    private fun getJobs(): List<Pair<String, String>> {
        return client.batch().v1().jobs().inNamespace(namespace)
            .list()
            .items
            .map {
                it.metadata.name to it.metadata.uid
            }
            .log()
    }

    private fun <R> Iterable<R>.log(): List<R> {
        return map {
            println(it)
            it
        }
    }

    private fun getPods(): List<Pair<String, String?>> {
        return client.pods().inNamespace(namespace)
            .list()
            .items
            .map {
                it.metadata.name to it.metadata.labels["controller-uid"]
            }
            .log()
    }

    private fun resolveSpec(executionTime: Long): String {
        return resolver.fill(
            jobSpecProvider.getTemplate(), ImageRunRequest.from(
                "busybox", "-c", listOf("echo 1 && sleep $executionTime && echo 2")
            )
        )
    }
}
