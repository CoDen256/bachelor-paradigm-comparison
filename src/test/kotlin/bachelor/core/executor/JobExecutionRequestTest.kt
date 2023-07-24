package bachelor.core.executor

import bachelor.millis
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

class JobExecutionRequestTest{
    @Test
    fun request() {
        val request = JobExecutionRequest("spec",
            millis(1000), millis(9999)
        )

        assertEquals("spec", request.jobSpec)
        assertEquals(millis(1000), request.isRunningTimeout)
        assertEquals(millis(9999), request.isTerminatedTimeout)
    }

    @Test
    fun invalidRequest() {
        assertThrows<IllegalArgumentException> {
            JobExecutionRequest("", millis(1000), millis(9999))
        }

        assertThrows<IllegalArgumentException> {
            JobExecutionRequest("s", millis(-1000), millis(9999))
        }
        assertThrows<IllegalArgumentException> {
            JobExecutionRequest("s", millis(1000), millis(-9999))
        }

        assertThrows<IllegalArgumentException> {
            JobExecutionRequest("s", millis(5000), millis(1000))
        }
    }
}