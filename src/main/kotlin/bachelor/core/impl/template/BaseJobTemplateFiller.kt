package bachelor.core.impl.template

import bachelor.core.ImageRunRequest
import bachelor.core.template.JobTemplateFiller
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
        const val NAME_PLACEHOLDER = "NAME"
        const val IMAGE_PLACEHOLDER = "IMAGE_NAME"
        const val NAMESPACE_PLACEHOLDER = "NAMESPACE"
        const val COMMAND_PLACEHOLDER = "COMMAND"
        const val TTL_PLACEHOLDER = "TTL"
        const val ACTIVE_DEADLINE_SECONDS_PLACEHOLDER = "ACTIVE_DEADLINE"
        const val ARGUMENTS_PLACEHOLDER = "ARGUMENTS"
    }


    override fun fill(template: String, spec: ImageRunRequest): String {
        val values = HashMap<String, String>(mapOf(
            NAME_PLACEHOLDER to spec.name,
            NAMESPACE_PLACEHOLDER to spec.namespace,
            IMAGE_PLACEHOLDER to spec.image,
        ))

        values[COMMAND_PLACEHOLDER] =  quote(spec.command ?: "/bin/sh")
        values[TTL_PLACEHOLDER] = "${spec.ttl ?: 0}"
        values[ACTIVE_DEADLINE_SECONDS_PLACEHOLDER] = "${spec.activeDeadlineSeconds ?: 1000}"
        values[ARGUMENTS_PLACEHOLDER] = substituteArguments(spec.arguments.toMutableList())

        return StringSubstitutor.replace(template, values)
    }

    override fun fill(template: String, values: Map<String, String>): String {
        return StringSubstitutor.replace(template, values)
    }

    private fun substituteArguments(arguments: List<String>): String {
        return arguments.joinToString(", ") { quote(it) }
    }

    private fun quote(it: String) = "\"${it.replace("\"", "\\\"")}\""
}