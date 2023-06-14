package calculations.runner.config

import calculations.runner.config.properties.KubernetesJobRunnerProperties
import calculations.runner.kubernetes.*
import calculations.runner.kubernetes.api.JobApi
import calculations.runner.kubernetes.executor.KubernetesJobExecutor
import calculations.runner.kubernetes.template.JobTemplateFiller
import calculations.runner.kubernetes.template.JobTemplateProvider
import calculations.runner.run.ImageRunner
import io.fabric8.kubernetes.client.Config
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
@EnableConfigurationProperties(KubernetesJobRunnerProperties::class)
class KubernetesJobRunnerConfiguration {

    /**
     * The proper configuration will be automatically created and found by the
     * [ConfigBuilder] if the runner is called within a Kubernetes Cluster.
     * It will find the necessary kubeconfig file as well as service account
     * tokens. No additional configuration is required, except for the presence
     * of a Service Account.
     *
     * A Service Account should be created within the namespace
     * the service will be running in, with following permissions:
     * [ "get", "list", "watch", "create", "update", "patch", "delete" ] for
     * following resources: [ "jobs", "pods", "pods/log" ]. The service account
     * should also be referred to in the corresponding deployment spec file
     * under .spec.template.spec.serviceAccountName
     */
    @Bean
    fun kubernetesConfig(): Config {
        return ConfigBuilder()
            .build()
    }

    @Bean
    fun kubernetesClient(config: Config): KubernetesClient {
        return KubernetesClientBuilder()
            .withConfig(config)
            .build()
    }

    @Bean
    fun jobTemplateResolver(): JobTemplateFiller {
        return BaseJobTemplateFiller()
    }

    @Bean
    fun jobTemplateProvider(properties: KubernetesJobRunnerProperties): JobTemplateProvider {
        return JobTemplateFileLoader(properties.templateFile)
    }

    @Bean
    fun kubernetesJobApiClient(client: KubernetesClient, properties: KubernetesJobRunnerProperties): JobApi {
        return BaseJobApi(client, properties.namespace)
    }

    @Bean
    fun kubernetesJobExecutor(jobApi: JobApi): KubernetesJobExecutor {
        return KubernetesJobExecutor(jobApi)
    }

    /**
     * [KubernetesJobImageRunner] is an [ImageRunner], which runs images based
     * on the execution of the Kubernetes jobs in the cluster.
     *
     * There are 5 different timeout relating properties, that can be tweaked
     * to control, how much the job is allowed to execute (in order of
     * preference, the first ones are more preferable to use, and should be
     * smaller than the subsequent ones):
     * - runner.kube.runningTimeoutMillis - the timeout for the
     *   [KubernetesJobImageRunner] to internally control the amount of time
     *   allowed for a job to reach the RUNNING state. If the job's pod does
     *   not reach the state RUNNING within the specified deadlined, it will
     *   return the latest available state of the execution and terminate with
     *   an exception
     * - runner.kube.terminationTimeoutMillis - a similar timeout for the
     *   [KubernetesJobImageRunner], that defines how much time is allowed for
     *   a job's pod to reach TERMINATED state. Once exceeded it will return an
     *   exception with the latest available state
     * - activeDeadlineSeconds - the job spec property defined in the job
     *   template YAML-file under .spec.activeDeadlineSeconds. This is a
     *   kubernetes native approach to set a deadline for a job that specifies
     *   how long it is allowed for the job to execute. Once it is exceeded,
     *   Kubernetes marks the job as Failed and tries to terminate the pod.
     * - executor.taskTimeoutMillis - the timeout used by
     *   [SequentialByKeyExecutor] to define how long a task in the queue
     *   should be allowed to execute. Once its exceeded, the task submitted to
     *   the executor is cancelled and the next one starts to execute
     * - spring.mvc.async.request-timeout - the timeout used by Spring, that
     *   defines a request timeout.
     *
     * It is preferable to specify runningTimeoutMillis and
     * terminationTimeoutMillis over other timeouts, because they
     * provide much more control and information in case of a timeout.
     */
    @Bean
    fun runner(
        executor: KubernetesJobExecutor,
        templateResolver: JobTemplateFiller,
        templateProvider: JobTemplateProvider,
        properties: KubernetesJobRunnerProperties
    ): ImageRunner {
        return KubernetesJobImageRunner(
            executor,
            templateProvider,
            templateResolver,
            Duration.ofMillis(properties.runningTimeoutMillis),
            Duration.ofMillis(properties.terminationTimeoutMillis)
        )
    }

}