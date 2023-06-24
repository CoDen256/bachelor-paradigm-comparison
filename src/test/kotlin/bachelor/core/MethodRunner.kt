package bachelor.core

import bachelor.executor.reactive.ReactiveJobExecutor
import bachelor.core.impl.api.fabric8.Fabric8ReactiveJobApi
import bachelor.core.impl.template.BaseJobTemplateFiller
import bachelor.core.impl.template.JobTemplateFileLoader
import bachelor.core.ImageRunRequest
import bachelor.core.KubernetesBasedImageRunner
import com.google.gson.reflect.TypeToken
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.apis.BatchV1Api
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1Namespace
import io.kubernetes.client.util.ClientBuilder
import io.kubernetes.client.util.Config
import io.kubernetes.client.util.Watch
import org.junit.jupiter.api.Test
import java.io.File
import java.lang.Boolean
import java.time.Duration
import java.util.concurrent.TimeUnit


class MethodRunner {


    private val client = KubernetesClientBuilder()
        .withConfig(ConfigBuilder().build()).build()

    private val api = Fabric8ReactiveJobApi(client, "calculations")

    private val templateLoader = JobTemplateFileLoader(
        File("template.yaml")
    )

    private val templateFiller = BaseJobTemplateFiller()

    @Test
    fun reactiveJobExecutor() {
        api.startListeners()

        val executor = ReactiveJobExecutor(api)
        KubernetesBasedImageRunner(executor, templateLoader, templateFiller)
            .run(
                ImageRunRequest.from("test-rscript", "main.R", listOf("1", "2", "3"),"latest"),
                Duration.ofSeconds(50), Duration.ofSeconds(50),
            ).doOnEach {
                println(it)
            }.subscribe {
                println(it)
            }
    }

    @Test
    fun imperativeJobExecutor() {
        return
        val apiClient = ClientBuilder.standard().build()
//        apiClient.

        val client = Config.defaultClient()
        // infinite timeout
        // infinite timeout
        val httpClient = client.httpClient.newBuilder().readTimeout(0, TimeUnit.SECONDS).build()
        client.setHttpClient(httpClient)
        Configuration.setDefaultApiClient(client)

        val api = CoreV1Api()

        val batchApi = BatchV1Api()
        val res = batchApi.listNamespacedJob("client-test", null, null, null, null, null, null, null, null,
            null, false)

        val items = res.items // same as fabric8
        return

        val watch = Watch.createWatch<V1Namespace>(
            client,
            api.listNamespaceCall(
                null, null, null, null, null, 5, null,
                null, null,
                Boolean.TRUE, null
            ),
            object : TypeToken<Watch.Response<V1Namespace?>?>() {}.type
        )

        try {
            for (item in watch) {
                System.out.printf("%s : %s%n", item.type, item.`object`.metadata!!.name)
            }
        } finally {
            watch.close()
        }
    }
}