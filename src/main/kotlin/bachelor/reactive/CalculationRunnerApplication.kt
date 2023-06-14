package calculations.runner

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication


@SpringBootApplication
class CalculationRunnerApplication

fun main(args: Array<String>) {
    runApplication<CalculationRunnerApplication>(*args)
}
