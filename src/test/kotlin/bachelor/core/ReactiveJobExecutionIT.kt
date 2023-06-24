//package bachelor.core
//
//import bachelor.core.api.ReactiveJobApi
//import bachelor.core.impl.api.fabric8.Fabric8ReactiveJobApi
//import bachelor.core.impl.template.BaseJobTemplateFiller
//import bachelor.core.impl.template.JobTemplateFileLoader
//import bachelor.executor.reactive.ReactiveJobExecutor
//import io.fabric8.kubernetes.client.ConfigBuilder
//import io.fabric8.kubernetes.client.KubernetesClientBuilder
//import org.junit.jupiter.api.BeforeEach
//import org.junit.jupiter.api.Test
//import java.io.File
//import java.time.Duration
//
//
//class ReactiveJobExecutionIT {
//
//
//    private val client = KubernetesClientBuilder()
//        .withConfig(ConfigBuilder().build()).build()
//
//    private val resolver = BaseJobTemplateFiller()
//    private val jobSpecFile = "/template/job.yaml"
//    private val jobSpecProvider = JobTemplateFileLoader(File(this::class.java.getResource(jobSpecFile)!!.toURI()))
//
//
//    private lateinit var api: ReactiveJobApi
//
//    @BeforeEach
//    fun startup(){
//
//    }
//
//    @Test
//    fun reactiveJobExecutor() {
//        api.startListeners()
//
//        val executor = ReactiveJobExecutor(api)
//        val request = ImageRunRequest.from("test-rscript", "main.R", listOf("1", "2", "3"), "latest")
//        val runningTimeout = Duration.ofSeconds(50)
//        val terminatedTimeout = Duration.ofSeconds(50)
//        KubernetesBasedImageRunner(executor, templateLoader, templateFiller)
//            .run(
//                request,
//                runningTimeout, terminatedTimeout,
//            ).doOnEach {
//                println(it)
//            }.subscribe {
//                println(it)
//            }
//        api.stopListeners()
//    }
//
//}