package bachelor.service.template

import calculations.runner.kubernetes.template.JobTemplateProvider
import org.apache.logging.log4j.LogManager
import java.io.File

/**
 * The [JobTemplateFileLoader] loads a job template from the given file
 * path.
 *
 * @property templateFile the file of the template to provide
 */
class JobTemplateFileLoader(private val templateFile: File) : JobTemplateProvider {

    private val logger = LogManager.getLogger()

    init {
        if (!templateFile.exists()) {
            logger.error("Calculation job template file does not exist: $templateFile")
            throw IllegalArgumentException("Calculation job template file does not exist: $templateFile")
        }
    }

    override fun getTemplate(): String {
        return templateFile.readText()
    }
}