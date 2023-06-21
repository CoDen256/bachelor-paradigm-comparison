package bachelor

import bachelor.reactive.kubernetes.ReactiveJobExecutor
import bachelor.service.api.ReactiveFabric8JobApi
import bachelor.service.run.ImageRunRequest
import bachelor.service.run.KubernetesBasedImageRunner
import bachelor.service.utils.BaseJobTemplateFiller
import bachelor.service.utils.JobTemplateFileLoader
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.junit.jupiter.api.Test
import java.io.File
import java.time.Duration

class MethodRunner {


    private val client = KubernetesClientBuilder()
        .withConfig(ConfigBuilder().build()).build()

    private val api = ReactiveFabric8JobApi(client, "calculations")

    private val templateLoader = JobTemplateFileLoader(
        File("template.yaml")
    )

    private val templateFiller = BaseJobTemplateFiller()

    @Test
    fun reactiveJobExecutor() {
        api.start()

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
}