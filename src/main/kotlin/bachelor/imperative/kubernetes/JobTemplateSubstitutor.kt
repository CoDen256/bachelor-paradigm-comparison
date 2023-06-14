package calculations.runner.kubernetes

import calculations.runner.run.ImageSpec

/**
 * The [JobTemplateSubstitutor] generates a job definition by substituting
 * variables in the Kubernetes Job Template by values from [ImageSpec].
 */
interface JobTemplateSubstitutor {
    /**
     * Populate given job template by substituting all variable placeholders
     * with the data from given [ImageSpec]
     *
     * @param jobTemplate the Kubernetes Job Template containing placeholders
     *     to substitute
     * @param spec the [ImageSpec], containing actual values for a job spec
     * @return a kubernetes job definition for the given spec.
     */
    fun substitute(jobTemplate: String, spec: ImageSpec): String
}

