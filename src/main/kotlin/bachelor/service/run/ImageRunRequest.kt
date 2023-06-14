package calculations.runner.run

/**
 * The [ImageRunRequest] defines a request to run a specific image. It
 * specifies:
 * - what image should be run - identified by the image name and the image
 *   tag
 * - what script within the image should be invoked - identified by the
 *   script name
 * - what arguments should be passed upon invoking the script within an
 *   image - a list of string arguments
 *
 * @property name the name of the image
 * @property script the script inside the image working directory, or an
 *     absolute path to the script
 * @property arguments the list of arguments, that will be passed to script
 *     upon invoking
 * @property tag the tag of the image, "latest" by default
 */
data class ImageRunRequest(val name: String, val script: String, val arguments: List<String>, val tag: String) {

    override fun toString(): String {
        return "$name:$tag/$script[$arguments]"
    }

    companion object {
        fun from(imageName: String, script: String, arguments: List<String>, tag: String? = null): ImageRunRequest {
            return ImageRunRequest(imageName, script, arguments, tag ?: "latest")
        }
    }

}