package calculations.runner.config

import calculations.runner.config.properties.ExecutorProperties
import calculations.runner.executor.SequentialByKeyExecutor
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
@EnableConfigurationProperties(ExecutorProperties::class)
class ExecutorConfiguration {
    @Bean
    fun executor(properties: ExecutorProperties): SequentialByKeyExecutor<String> {
        return SequentialByKeyExecutor(Duration.ofMillis(properties.taskTimeoutMillis))
    }
}