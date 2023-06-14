package calculations.runner.run

import java.util.concurrent.CompletableFuture

/**
 * The [ImageRunner] executes a specific image or, in other words, a script
 * within the image, with a given set of arguments, reads the output of
 * the execution as [String] and returns it to the caller as a result of
 * asynchronous computation.
 *
 * The image to be executed, its version(tag), the target script and the
 * arguments to pass are provided via [ImageSpec]
 *
 * The callers of the [ImageRunner] should be unaware what platform or
 * engine is used to run the image. It could be Docker Daemon, Kubernetes
 * Jobs or any other tool.
 */
interface ImageRunner {
    /**
     * Run an image defined by [ImageSpec] and return the result of execution.
     *
     * @param spec the image spec, defining an image, script and arguments to
     *     run.
     * @return the result of the execution
     */
    fun run(spec: ImageSpec): CompletableFuture<String>
}