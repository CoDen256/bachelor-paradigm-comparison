package calculations.runner.kubernetes.template

import calculations.runner.run.ImageSpec
import calculations.runner.kubernetes.JobTemplateSubstitutor
import org.apache.commons.text.StringSubstitutor

/**
 * The [BaseJobTemplateSubstitutor] is a basic implementation of the
 * [JobTemplateSubstitutor]. Given a job template and an [ImageSpec], it
 * substitutes placeholders in the template by values from the spec and
 * generates a Kubernetes Job Definition The substitutor expects a template
 * with only 3 variables to substitute:
 * - CALCULATION_IMAGE_NAME - the name of the image
 * - CALCULATION_IMAGE_TAG - the tag of the image
 * - ARGUMENTS - the arguments that will be passed to the image. The first
 *   argument refers to the name of the script file to run, and other arguments
 *   are passed to the script itself
 */
class BaseJobTemplateSubstitutor : JobTemplateSubstitutor {

    companion object {
        const val IMAGE_PLACEHOLDER = "CALCULATION_IMAGE_NAME"
        const val TAG_PLACEHOLDER = "CALCULATION_IMAGE_TAG"
        const val ARGUMENTS_PLACEHOLDER = "ARGUMENTS"
    }


    override fun substitute(jobTemplate: String, spec: ImageSpec): String {
        return StringSubstitutor.replace(
            jobTemplate,
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