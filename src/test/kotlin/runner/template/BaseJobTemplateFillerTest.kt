package runner.template

import bachelor.service.template.BaseJobTemplateFiller
import calculations.runner.run.ImageRunRequest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BaseJobTemplateFillerTest {

    private val substitutor = BaseJobTemplateFiller()
    private val imagePlaceholder = "\${CALCULATION_IMAGE_NAME}"
    private val argPlaceholder = "\${ARGUMENTS}"
    private val tagPlaceholder = "\${CALCULATION_IMAGE_TAG}"

    @Test
    fun substituteWithoutArgs() {
        val result = substitutor.fill(
            "$imagePlaceholder:[ $argPlaceholder ]",
            ImageRunRequest.from("imageName", "script", listOf())
        )
        assertEquals("imageName:[ \"script\" ]", result)
    }

    @Test
    fun substituteWithOneArg() {
        val result = substitutor.fill("$imagePlaceholder:\n[ $argPlaceholder ]",
            ImageRunRequest.from("image name", "script 2.py", listOf(
                "arg0"
            ))
        )
        assertEquals("image name:\n[ \"script 2.py\", \"arg0\" ]", result)
    }

    @Test
    fun substituteWithCustomTag() {
        val result = substitutor.fill("$imagePlaceholder:$tagPlaceholder\n[ $argPlaceholder ]",
            ImageRunRequest.from("image name", "script 2.py", listOf(
                "arg0"
            ), "1.10")
        )
        assertEquals("image name:1.10\n[ \"script 2.py\", \"arg0\" ]", result)
    }

    @Test
    fun substituteWithComplexArguments() {
        val result = substitutor.fill("$imagePlaceholder\n- args : [ $argPlaceholder ]",
            ImageRunRequest.from("test-script", "main.R", listOf(
                "NPS", " 01-01-2001 10:00:00 ", """ -extraarg="extra arg" """
            ))
        )
        assertEquals("test-script\n" +
                """- args : [ "main.R", "NPS", " 01-01-2001 10:00:00 ", " -extraarg=\"extra arg\" " ]""",
            result)
    }

    @Test
    fun substituteFullTemplateFile() {
        val template = BaseJobTemplateFillerTest::class.java.getResource("/template/template.yaml")!!.readText()
        val expected = BaseJobTemplateFillerTest::class.java.getResource("/template/template-values.yaml")!!.readText()


        val result = substitutor.fill(template,
            ImageRunRequest.from("test-script", "script.py", listOf("1", "10-10-2000 10:00:00", " hustensaft "), "1.0"),
        )
        assertEquals(expected, result)
    }

}