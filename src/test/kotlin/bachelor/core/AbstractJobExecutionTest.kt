package bachelor.core

import bachelor.core.api.Action
import bachelor.core.api.JobApi
import bachelor.core.api.snapshot.*
import bachelor.core.executor.JobExecutionRequest
import bachelor.core.executor.JobExecutor
import bachelor.core.utils.generate.TARGET_JOB
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.whenever
import java.lang.IllegalArgumentException
import java.time.Duration


abstract class AbstractJobExecutionTest (
    val createExecutor: (JobApi) -> JobExecutor
){

    private val namespace = "ns"
    private val JOB_NAME = TARGET_JOB
    private val JOB_SPEC = ""

    private lateinit var api: JobApi
    private lateinit var executor: JobExecutor

    @BeforeEach
    fun startup() {
        executor = createExecutor(api)
    }

        @Test
    fun givenInvalidDuration_whenExecuted_thenEmptyExecutionSnapshot() {

        assertThrows<IllegalArgumentException> {  }
        val snapshot = executor.execute(
            JobExecutionRequest(JOB_SPEC,
                Duration.ofMillis(-1),
                Duration.ofMillis(-1))
        )

        //

    }

//    @Test
//    fun givenNoEvents_whenMaxTimeout_thenEmptyExecutionSnapshot() {
//        // when
//        whenever(api.create(JOB_SPEC)).thenReturn(JobR)
//
//        // execute
//        val snapshot = executor.execute(
//            JobExecutionRequest(JOB_SPEC,
//                Duration.ofMillis(-1),
//                Duration.ofMillis(-1))
//        )
//
//        //
//
//    }

//    @Test
//    fun givenNoEvents_whenMaxTimeout_thenEmptyExecutionSnapshot() {
//        // when
//        whenever(api.create(JOB_SPEC)).thenReturn(JobR)
//
//        // execute
//        val snapshot = executor.execute(
//            JobExecutionRequest(JOB_SPEC,
//            Duration.ofMillis(-1),
//                Duration.ofMillis(-1))
//        )
//
//        //
//
//    }


    private fun JobExecutor.execute(
        runningTimeout: Long = 10_000,
        terminatedTimeout: Long = 10_000,
    ): ExecutionSnapshot {
        return execute(JobExecutionRequest(
            JOB_SPEC,
            Duration.ofMillis(runningTimeout),
            Duration.ofMillis(terminatedTimeout)
        ))
    }

    fun job(
        uid: String,
        action: Action,
        active: Int? = null,
        ready: Int? = null,
        failed: Int? = null,
        succeeded: Int? = null,
        conditions: List<String> = listOf()
    ): ActiveJobSnapshot = ActiveJobSnapshot(
        JOB_NAME,
        uid,
        namespace,
        conditions.map { JobCondition("True", JOB_SPEC, it, JOB_SPEC) },
        JobStatus(active, ready, failed, succeeded),
        action.name
    )

    fun pod(
        name: String,
        uid: String,
        jobUid: String,
        action: Action,
        phase: Phase,
        state: ContainerState = UnknownState
    ): ActivePodSnapshot =
        ActivePodSnapshot(name, uid, namespace, jobUid, state, phase, action.name)
}