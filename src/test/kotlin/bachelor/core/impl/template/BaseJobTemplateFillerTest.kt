package bachelor.core.impl.template

import bachelor.core.ImageRunRequest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BaseJobTemplateFillerTest {

    private val substitutor = BaseJobTemplateFiller()
    private val imagePlaceholder = "\${IMAGE_NAME}"
    private val argPlaceholder = "\${ARGUMENTS}"
    private val namePlaceholder = "\${NAME}"

    @Test
    fun substituteWithoutArgs() {
        val result = substitutor.fill(
            "$imagePlaceholder:[ $argPlaceholder ]",
            ImageRunRequest("", "", "imageName", arguments = listOf("script"))
        )
        assertEquals("imageName:[ \"script\" ]", result)
    }

    @Test
    fun substituteWithOneArg() {
        val result = substitutor.fill("$imagePlaceholder:\n[ $argPlaceholder ]",
            ImageRunRequest("", "", "image name",  arguments = listOf(
                "script 2.py",
                "arg0"
            ))
        )
        assertEquals("image name:\n[ \"script 2.py\", \"arg0\" ]", result)
    }

    @Test
    fun substituteWithCustomTag() {
        val result = substitutor.fill("$namePlaceholder/$imagePlaceholder\n[ $argPlaceholder ]",
            ImageRunRequest("job", "", "image name:1.10", arguments = listOf(
                "script 2.py",
                "arg0"
            ))
        )
        assertEquals("job/image name:1.10\n[ \"script 2.py\", \"arg0\" ]", result)
    }

    @Test
    fun substituteWithComplexArguments() {
        val result = substitutor.fill("$imagePlaceholder\n- args : [ $argPlaceholder ]",
            ImageRunRequest("", "", "test-script", arguments = listOf(
                "main.R", "NPS", " 01-01-2001 10:00:00 ", """ -extraarg="extra arg" """
            ))
        )
        assertEquals("test-script\n" +
                """- args : [ "main.R", "NPS", " 01-01-2001 10:00:00 ", " -extraarg=\"extra arg\" " ]""",
            result)
    }

    @Test
    fun substituteFullTemplateFile() {
        val template = BaseJobTemplateFillerTest::class.java.getResource("/template/job.yaml")!!.readText()
        val expected = BaseJobTemplateFillerTest::class.java.getResource("/template/job-values.yaml")!!.readText()


        val result = substitutor.fill(template,
            ImageRunRequest(
                "test-script-job",
                "default",
                "docker.optimax.cloud/kubernetes/test-script:1.0",
                "python",
                20,
                2000,
                listOf("script.py", "1", "10-10-2000 10:00:00", " hustensaft "))
        )
        assertEquals(expected, result)
    }

    @Test
    fun substituteDefaultTemplateFile() {
        val template = BaseJobTemplateFillerTest::class.java.getResource("/template/job.yaml")!!.readText()
        val expected = BaseJobTemplateFillerTest::class.java.getResource("/template/job-default.yaml")!!.readText()


        val result = substitutor.fill(template,
            ImageRunRequest(
                "test-script-job",
                "default",
                "docker.optimax.cloud/kubernetes/test-script:1.0")
        )
        assertEquals(expected, result)
    }

}