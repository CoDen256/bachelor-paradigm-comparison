package calculations.runner.config

import calculations.runner.service.CalculationRunnerService
import calculations.runner.service.ClientException
import calculations.runner.service.ServerException
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.*
import org.springframework.web.reactive.function.server.RequestPredicates.accept
import org.springframework.web.reactive.function.server.RouterFunctions.route
import org.springframework.web.reactive.function.server.ServerResponse.*
import reactor.core.publisher.Mono


@Configuration
class RouterConfiguration {

    @Bean
    fun execute(service: CalculationRunnerService): RouterFunction<ServerResponse> {
        return route()
            .POST("/execute/{name}/{script}", accept(MediaType.APPLICATION_JSON))
            { request -> executeRunImageRequest(request, service) }
            .build()
    }


    /**
     * Executes a request to run an image. The request contains the name of
     * the image, the tag of the image, the script to run, and arguments to
     * pass, encoded as JSON ARRAY in the body of the request. If the request
     * contains no body, an empty list is used by default. Request contains:
     * - name - a path variable, specifies the name of the image
     * - script - a path variable, specifies the name of the script file within
     *   the image
     * - arguments - an optional request body in JSON ARRAY format, specifies
     *   the arguments that will be passed to the script
     * - tag - an optional query parameter, specifies the tag of the image
     *
     * The request is then delegated to [CalculationRunnerService], which, in
     * turn, runs an image according to the request and collects its output.
     *
     * In case of success, the output of the image execution is then returned
     * in the text/plain format in the body of the response.
     *
     * All produced [ClientException]s will result in a BAD REQUEST response,
     * containing an error message in the response body in the text/plain
     * format
     *
     * All produced [ServerException]s will result in an INTERNAL SERVER ERROR
     * response, containing an error message in the response body in the
     * text/plain format
     *
     * @param request the request to run an image
     * @param service the runner service, that executes the request and runs an
     *     image.
     * @return a response, containing the output of successful image execution
     */
    fun executeRunImageRequest(request: ServerRequest, service: CalculationRunnerService): Mono<ServerResponse> {
        val name = request.pathVariable("name")
        val script = request.pathVariable("script")
        val tag = request.queryParamOrNull("tag")
        return getArgumentsFromBody(request)
            .switchIfEmpty(Mono.just(emptyList()))
            .flatMap { args ->
                service.execute(name, script, args, tag)
            }.flatMap { output: String ->
                ok().contentType(MediaType.TEXT_PLAIN).bodyValue(output)
            }.onErrorResume(ClientException::class.java) {
                badRequest()
                    .contentType(MediaType.TEXT_PLAIN)
                    .bodyValue(it.message)
            }.onErrorResume(ServerException::class.java) {
                status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .contentType(MediaType.TEXT_PLAIN)
                    .bodyValue(it.message)
            }
    }

    private fun getArgumentsFromBody(request: ServerRequest): Mono<List<String>> =
        request.bodyToMono()
}