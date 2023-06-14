package calculations.runner.kubernetes

import calculations.runner.kubernetes.template.JobTemplateFiller
import calculations.runner.run.ImageRunRequest
import org.apache.commons.text.StringSubstitutor

/**
 * The [BaseJobTemplateFiller] is a basic implementation of the
 * [JobTemplateFiller]. Given a job template and an [ImageRunRequest], it
 * substitutes placeholders in the template by values from the spec and
 * generates a Kubernetes Job Definition The substitutor expects a template
 * with only 3 variables to substitute:
 * - CALCULATION_IMAGE_NAME - the name of the image
 * - CALCULATION_IMAGE_TAG - the tag of the image
 * - ARGUMENTS - the arguments that will be passed to the image. The first
 *   argument refers to the name of the script file to run, and other
 *   arguments are passed to the script itself
 */
class BaseJobTemplateFiller : JobTemplateFiller {

    companion object {
        const val IMAGE_PLACEHOLDER = "CALCULATION_IMAGE_NAME"
        const val TAG_PLACEHOLDER = "CALCULATION_IMAGE_TAG"
        const val ARGUMENTS_PLACEHOLDER = "ARGUMENTS"
    }


    override fun fill(template: String, spec: ImageRunRequest): String {
        return StringSubstitutor.replace(
            template,
            mapOf(
                IMAGE_PLACEHOLDER to spec.name,
                TAG_PLACEHOLDER to spec.tag,
                ARGUMENTS_PLACEHOLDER to substituteArguments(spec.arguments.toMutableList().also {
                    it.add(0, spec.script)
                })
            )
        )
    }

    private fun substituteArguments(arguments: List<String>): String {
        return arguments.joinToString(", ") {
            "\"${it.replace("\"", "\\\"")}\""
        }
    }
}