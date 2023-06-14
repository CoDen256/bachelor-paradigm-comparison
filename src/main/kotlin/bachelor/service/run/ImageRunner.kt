package bachelor.service.run

import calculations.runner.run.ImageRunRequest

/**
 * The [ImageRunner] executes a specific image or, in other words, a script
 * within the image, with a given set of arguments, reads the output of
 * the execution as [String] and returns it to the caller as a result of
 * asynchronous computation.
 *
 * The image to be executed, its version(tag), the target script and the
 * arguments to pass are provided via [ImageRunRequest]
 *
 * The callers of the [ImageRunner] should be unaware what platform or
 * engine is used to run the image. It could be a Docker Daemon, Kubernetes
 * Jobs or any other tool.
 */
interface ImageRunner {
    /**
     * Run an image defined by [ImageRunRequest] and return the result of
     * execution.
     *
     * @param request the image request, defining an image, script and
     *     arguments to run.
     * @return the result of the execution
     */
    fun run(request: ImageRunRequest): String?
}