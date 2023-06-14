package calculations.runner.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties


/**
 * The configuration properties for the [SequentialByKeyExecutor], that
 * enqueues and executes tasks provided by [ImageRunner]
 *
 * @property taskTimeoutMillis the deadline within which the [ImageRunner]
 *     tasks have to be completed
 */
@ConfigurationProperties(prefix = "executor")
data class ExecutorProperties(
    var taskTimeoutMillis: Long = 5 * 60 * 1000
)