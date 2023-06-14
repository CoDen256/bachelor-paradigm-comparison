package calculations.runner.config

import calculations.runner.config.properties.KubernetesJobRunnerProperties
import calculations.runner.kubernetes.*
import calculations.runner.kubernetes.api.LoggingJobApiClient
import calculations.runner.kubernetes.template.BaseJobTemplateSubstitutor
import calculations.runner.kubernetes.template.JobTemplateFileLoader
import calculations.runner.run.ImageRunner
import io.fabric8.kubernetes.client.Config
import io.fabric8.kubernetes.client.ConfigBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(KubernetesJobRunnerProperties::class)
class KubernetesJobRunnerConfiguration {

    /**
     * The proper configuration will be automatically created and found by the
     * [ConfigBuilder] if the runner is called within the Kubernetes Cluster.
     * It will find the needed kubeconfig file as well as service account
     * tokens. No additional configuration is required, except for a Service
     * Account.
     *
     * A Service Account should be created within the namespace
     * the service will be running in with following permissions:
     * [ "get", "list", "watch", "create", "update", "patch", "delete" ] for
     * following resources: [ "jobs", "pods", "pods/log" ]. The service account
     * should also be referred in the corresponding deployment file under
     * .spec.template.spec.serviceAccountName
     *
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
    fun jobTemplateResolver(): JobTemplateSubstitutor {
        return BaseJobTemplateSubstitutor()
    }

    @Bean
    fun jobTemplateProvider(properties: KubernetesJobRunnerProperties): JobTemplateProvider {
        return JobTemplateFileLoader(properties.templateFile)
    }

    @Bean
    fun kubernetesJobApiClient(client: KubernetesClient): JobApiClient {
        return LoggingJobApiClient(client)
    }

    @Bean
    fun runner(
        jobApiClient: JobApiClient,
        templateResolver: JobTemplateSubstitutor,
        templateProvider: JobTemplateProvider,
        properties: KubernetesJobRunnerProperties
    ): ImageRunner {
        return KubernetesJobRunner(
            jobApiClient,
            templateProvider,
            templateResolver,
            properties.readyTimeoutMillis,
            properties.terminationTimeoutMillis
        )
    }

}