package calculations.runner.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import java.io.File

/**
 * Properties for [KubernetesJobImageRunner], which runs images via
 * kubernetes Jobs
 *
 * @property templateFile the path to a template file, where job spec
 *     template is stored
 * @property runningTimeoutMillis the maximal amount of time allocated for
 *     a job(and related pod) to be in a RUNNING state
 * @property terminationTimeoutMillis the maximal amount of time allocated
 *     for a job(and related pod) to be in a TERMINATED state
 * @property namespace the namespace where the observation of the job and
 *     pod events should be performed
 */
@ConfigurationProperties(prefix = "runner.kube")
data class KubernetesJobRunnerProperties(
    var templateFile: File = File("./template.yaml"),
    var runningTimeoutMillis: Long = 60 * 1000,
    var terminationTimeoutMillis: Long = 2 * 60 * 1000,
    var namespace: String = "calculations"
)