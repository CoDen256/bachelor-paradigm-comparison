package calculations.runner.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "executor")
data class ExecutorProperties(
    var timeoutMillis: Long = 5 * 60 * 1000
)