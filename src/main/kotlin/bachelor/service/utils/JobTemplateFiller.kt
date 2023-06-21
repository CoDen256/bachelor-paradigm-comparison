package bachelor.service.utils

import bachelor.service.run.ImageRunRequest

/**
 * The [JobTemplateFiller] generates a job definition (job spec) by
 * filling variables in the Kubernetes Job Template with values from
 * [ImageRunRequest].
 */
interface JobTemplateFiller {
    /**
     * Fill the given job template by substituting all variable placeholders
     * with the data from given [ImageRunRequest]
     *
     * @param template the Kubernetes Job Template containing placeholders to
     *     substitute
     * @param spec the [ImageRunRequest], containing actual values for a job
     *     spec
     * @return a kubernetes job definition for the given spec.
     */
    fun fill(template: String, spec: ImageRunRequest): String

    /**
     * Fill the given job template by substituting all variable placeholders
     * with the data from given map
     *
     * @param template the Kubernetes Job Template containing placeholders to
     *     substitute
     * @param values the values to substitute in the placeholders
     * @return a kubernetes job definition for the given spec.
     */
    fun fill(template: String, values: Map<String, String>): String
}

