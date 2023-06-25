package bachelor.core

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
data class ImageRunRequest(
    val name: String,
    val namespace: String,
    val image: String,
    val command: String? = null,
    val ttl: Long? = null,
    val activeDeadlineSeconds: Long? = null,
    val arguments: List<String> = listOf(),
) {

    override fun toString(): String {
        return "$name|$image$arguments"
    }

}