package calculations.runner.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import java.io.File

@ConfigurationProperties(prefix = "runner.kube")
data class KubernetesJobRunnerProperties(
    var templateFile: File = File("./template.yaml"),
    var readyTimeoutMillis: Long = 60 * 1000,
    var terminationTimeoutMillis: Long = 60 * 1000
)