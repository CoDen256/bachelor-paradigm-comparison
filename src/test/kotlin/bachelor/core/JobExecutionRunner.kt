package bachelor.core

import bachelor.core.impl.api.fabric8.Fabric8ReactiveJobApi
import bachelor.core.impl.template.BaseJobTemplateFiller
import bachelor.core.impl.template.JobTemplateFileLoader
import bachelor.executor.reactive.ReactiveJobExecutor
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.junit.jupiter.api.Test
import java.io.File
import java.time.Duration


class JobExecutionRunner {


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
        val request = ImageRunRequest.from("test-rscript", "main.R", listOf("1", "2", "3"), "latest")
        val runningTimeout = Duration.ofSeconds(50)
        val terminatedTimeout = Duration.ofSeconds(50)
        KubernetesBasedImageRunner(executor, templateLoader, templateFiller)
            .run(
                request,
                runningTimeout, terminatedTimeout,
            ).doOnEach {
                println(it)
            }.subscribe {
                println(it)
            }
        api.stopListeners()
    }

    @Test
    fun imperativeJobExecutor() {
//        val executor = ImperativeJobExecutor()
//        KubernetesBasedImageRunner(executor, templateLoader, templateFiller)
//            .run(
//                ImageRunRequest.from("test-rscript", "main.R", listOf("1", "2", "3"),"latest"),
//                Duration.ofSeconds(50), Duration.ofSeconds(50),
//            ).doOnEach {
//                println(it)
//            }.subscribe {
//                println(it)
//            }
    }
}