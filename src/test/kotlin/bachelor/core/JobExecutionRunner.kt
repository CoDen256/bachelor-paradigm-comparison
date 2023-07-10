package bachelor.core

import bachelor.core.api.ReactiveJobApiAdapter
import bachelor.core.impl.api.fabric8.Fabric8JobApi
import bachelor.core.impl.template.BaseJobTemplateFiller
import bachelor.core.impl.template.JobTemplateFileLoader
import bachelor.executor.reactive.ReactiveJobExecutor
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.junit.jupiter.api.Test
import java.io.File
import java.time.Duration


const val NAMESPACE = "executor-test"

class JobExecutionRunner {

    private val client = KubernetesClientBuilder().build()

    private val api = ReactiveJobApiAdapter(Fabric8JobApi(client, "calculations"))

    private val templateLoader = JobTemplateFileLoader(
        File("template.yaml")
    )

    private val templateFiller = BaseJobTemplateFiller()

    @Test
    fun reactiveJobExecutor() {
        api.startListeners()

        val executor = ReactiveJobExecutor(api)
        val request = ImageRunRequest("test-job", "executor-test", "test-rscript:latest", arguments =  listOf("main.R", "1", "2", "3"))
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