package calculations.runner.service

import calculations.runner.executor.SequentialByKeyExecutor
import calculations.runner.run.ImageRunRequest
import calculations.runner.run.ImageRunner
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * The [CalculationRunnerService] is a service, that runs a requested image
 * on the given [ImageRunner] engine.
 *
 * The service expects the name of the image, its version(tag), the script
 * that runs within the image and arguments that are provided to the
 * script. The request is packed as a [ImageRunRequest] and run by the
 * [ImageRunner].
 *
 * The request to run an image will be enqueued by the
 * [SequentialByKeyExecutor] and be run sequentially for the same images.
 *
 * The [ImageRunner] will then run the requested image, script with
 * arguments on the underlying engine (e.g. Kubernetes, Docker etc.).
 *
 * @property executor the sequential executor
 * @property imageRunner the runner engine
 */
@Service
class CalculationRunnerService(
    private val executor: SequentialByKeyExecutor<String>,
    private val imageRunner: ImageRunner,
) {

    fun execute(name: String, script: String, args: List<String>, tag: String?): Mono<String> {
        return executor.submit(name, imageRunner.run(ImageRunRequest.from(name, script, args, tag)))
            .onErrorMap { errorsToServerException(it, name) }
    }

    private fun errorsToServerException(throwable: Throwable, name: String): Throwable = when (throwable) {
        is ClientException, is ServerException -> throwable
        else -> ServerException("Error while executing task for $name: ${throwable.message}", throwable)
    }
}