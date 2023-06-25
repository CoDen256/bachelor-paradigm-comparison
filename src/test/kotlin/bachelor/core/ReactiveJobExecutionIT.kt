package bachelor.core

import bachelor.core.api.ReactiveJobApi
import bachelor.core.api.snapshot.ExecutionSnapshot
import bachelor.core.executor.JobExecutor
import bachelor.core.impl.api.fabric8.Fabric8ReactiveJobApi
import bachelor.core.impl.template.BaseJobTemplateFiller
import bachelor.core.impl.template.JobTemplateFileLoader
import bachelor.core.utils.generate.TARGET_JOB
import bachelor.core.utils.generate.TARGET_POD
import bachelor.createNamespace
import bachelor.executor.reactive.ReactiveJobExecutor
import bachelor.millis
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import java.io.File
import java.time.Duration


class ReactiveJobExecutionIT {

    private val client = KubernetesClientBuilder().build().apply {
        createNamespace(NAMESPACE)
    }
    private val resolver = BaseJobTemplateFiller()
    private val jobSpecFile = "/template/executor-test-job.yaml"
    private val jobSpecProvider = JobTemplateFileLoader(File(this::class.java.getResource(jobSpecFile)!!.toURI()))


    private lateinit var api: ReactiveJobApi
    private lateinit var executor: JobExecutor
    private lateinit var runner: KubernetesBasedImageRunner

    @BeforeEach
    fun startup(){
        api = Fabric8ReactiveJobApi(client, "executor-test")
        executor = ReactiveJobExecutor(api)
        runner = KubernetesBasedImageRunner(executor, jobSpecProvider, resolver)
    }



    @Test
    fun reactiveJobExecutor() {
        api.startListeners()

        val result = runner.run(1000L, 1000L,
            0,
            0,
            10,
            10)

        result.doOnEach {
                println(it)
            }.subscribe {
                println(it)
            }

        api.stopListeners()
    }

    private fun KubernetesBasedImageRunner.run(
        runningTimeout: Long,
        terminatedTimeout: Long,
        exitCode: Int,
        executionTime: Long,
        ttl: Long,
        activeDeadlineSeconds: Long
    ): Mono<ExecutionSnapshot> {
        return run(
            runningTimeout,
            terminatedTimeout,
            ttl,
            activeDeadlineSeconds,
            "echo start && sleep $executionTime && echo slept $executionTime && echo end && exit $exitCode"
            )
    }

    private fun KubernetesBasedImageRunner.run(
        runningTimeout: Long,
        terminatedTimeout: Long,
        ttl: Long,
        activeDeadlineSeconds: Long,
        vararg arguments: String
    ): Mono<ExecutionSnapshot> {
        return run(ImageRunRequest(
            TARGET_JOB,
            NAMESPACE,
            "busybox:latest",
            "/bin/sh",
            ttl,
            activeDeadlineSeconds,
            listOf("-c") + arguments
        ),
            millis(runningTimeout),
            millis(terminatedTimeout)
        )
    }

}