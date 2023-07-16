package bachelor.core

import bachelor.core.api.*
import bachelor.core.api.snapshot.*
import bachelor.core.executor.JobExecutionRequest
import bachelor.core.executor.JobExecutor
import bachelor.core.executor.PodNotRunningTimeoutException
import bachelor.core.utils.generate.TARGET_JOB
import bachelor.millis
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mock
import org.mockito.invocation.InvocationOnMock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.capture
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.stubbing.OngoingStubbing
import java.lang.IllegalArgumentException
import java.lang.IllegalStateException
import java.time.Duration
import kotlin.test.assertEquals

@ExtendWith(MockitoExtension::class)
abstract class AbstractJobExecutionTest (
    val createExecutor: (JobApi) -> JobExecutor
){

    private val namespace = "ns"
    private val JOB_NAME = TARGET_JOB
    private val JOB_SPEC = "spec"

    @Mock
    private lateinit var api: JobApi

    @Captor
    private lateinit var jobHandlerCaptor: ArgumentCaptor<ResourceEventHandler<ActiveJobSnapshot>>

    @Captor
    private lateinit var podHandlerCaptor: ArgumentCaptor<ResourceEventHandler<ActivePodSnapshot>>

    private lateinit var executor: JobExecutor

    @BeforeEach
    fun startup() {
        executor = createExecutor(api)
    }

    @Test
    fun givenFailedToAddJobEventHandler_whenExecuted_thenThrowExceptionAndUnsubscribe() {
        whenever(api.addJobEventHandler(any())).thenThrow(IllegalArgumentException())


        assertThrows<IllegalArgumentException> {
            executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(0)))
        }

        verify(api).addJobEventHandler(capture(jobHandlerCaptor))
        verify(api).removeJobEventHandler(jobHandlerCaptor.value)
        verify(api).removePodEventHandler(any())
    }

    @Test
    fun givenFailedToAddPodEventHandler_whenExecuted_thenThrowExceptionAndUnsubscribe() {
        whenever(api.addPodEventHandler(any())).thenThrow(IllegalStateException())

        assertThrows<IllegalStateException> {
            executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(0)))
        }

        verify(api).addJobEventHandler(capture(jobHandlerCaptor))
        verify(api).removeJobEventHandler(jobHandlerCaptor.value)

        verify(api).addPodEventHandler(capture(podHandlerCaptor))
        verify(api).removePodEventHandler(podHandlerCaptor.value)
    }

    @Test
    fun givenJobSpecIsInvalid_whenExecuted_thenThrowJobSpecIsInvalidExceptionAndUnsubscribe() {
        whenever(api.create(JOB_SPEC)).thenThrow(InvalidJobSpecException("", null))

        assertThrows<InvalidJobSpecException> {
            executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(0)))
        }
        verify(api).create(JOB_SPEC)

        verify(api).addJobEventHandler(capture(jobHandlerCaptor))
        verify(api).removeJobEventHandler(jobHandlerCaptor.value)

        verify(api).addPodEventHandler(capture(podHandlerCaptor))
        verify(api).removePodEventHandler(podHandlerCaptor.value)
    }

    @Test
    fun givenJobAlreadyExists_whenExecuted_thenThrowJobAlreadyExistsExceptionAndUnsubscribe() {
        whenever(api.create(JOB_SPEC)).thenThrow(JobAlreadyExistsException("", null))

        assertThrows<JobAlreadyExistsException> {
            executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(0)))
        }


        verify(api).create(JOB_SPEC)

        verify(api).addJobEventHandler(capture(jobHandlerCaptor))
        verify(api).removeJobEventHandler(jobHandlerCaptor.value)

        verify(api).addPodEventHandler(capture(podHandlerCaptor))
        verify(api).removePodEventHandler(podHandlerCaptor.value)
    }

    @Test
    fun givenNoEvents_whenExecuted_thenThrowPodNotRunningExceptionAndDeleteJobAndUnsubscribe() {
        val jobRef = JobReference("", "", "")
        whenever(api.create(JOB_SPEC)).thenReturn(jobRef)


        val ex = assertThrows<PodNotRunningTimeoutException> {
            executor.execute(JobExecutionRequest(JOB_SPEC, millis(0), millis(100)))
        }

        assertEquals(ex.currentState,
            ExecutionSnapshot(Logs.empty(), InitialJobSnapshot, InitialPodSnapshot))

        assertEquals(ex.timeout, millis(0))

        verify(api).create(JOB_SPEC)
        verify(api).delete(jobRef)

        verify(api).addJobEventHandler(capture(jobHandlerCaptor))
        verify(api).removeJobEventHandler(jobHandlerCaptor.value)

        verify(api).addPodEventHandler(capture(podHandlerCaptor))
        verify(api).removePodEventHandler(podHandlerCaptor.value)
    }

    //    @Test
//    fun givenInvalidSpecException_whenExecuted_thenThrowInvalidJobSpecException() {
//        // given
//        whenever(api.create(JOB_SPEC)).thenThrow(InvalidJobSpecException("", null))
//
//        //when, then
//        assertThrows<InvalidJobSpecException> {
//            executor.execute(
//                JobExecutionRequest(JOB_SPEC,
//                    millis(0),
//                    millis(0))
//            )
//        }
//    }
//
//    @Test
//    fun givenJobAlreadyExistsException_whenExecuted_thenThrowJobAlreadyExistsException() {
//        // given
//        whenever(api.create(JOB_SPEC)).thenThrow(JobAlreadyExistsException("", null))
//
//        //when, then
//        assertThrows<JobAlreadyExistsException> {
//            executor.execute(
//                JobExecutionRequest(JOB_SPEC,
//                    millis(0),
//                    millis(0))
//            )
//        }
//    }

//    @Test
//    fun givenNoEvents_whenMaxTimeout_thenEmptyExecutionSnapshot() {
//        // when
//        whenever(api.create(JOB_SPEC)).thenReturn(JobR)
//
//        // execute
//        val snapshot = executor.execute(
//            JobExecutionRequest(JOB_SPEC,
//                millis(-1),
//                millis(-1))
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
//            millis(-1),
//                millis(-1))
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
            millis(runningTimeout),
            millis(terminatedTimeout)
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

    private fun <T> OngoingStubbing<T>.thenCaptureJobHandler(answer: (InvocationOnMock) -> Any): ResourceEventHandler<ActiveJobSnapshot> {
        var ret: ResourceEventHandler<ActiveJobSnapshot>? = null
        then {
            ret = (it.arguments[0] as ResourceEventHandler<ActiveJobSnapshot>)
            answer(it)
        }
        return ret!!
    }

    private fun <T> OngoingStubbing<T>.thenCapturePodHandler(answer: (InvocationOnMock) -> Any): ResourceEventHandler<ActivePodSnapshot> {
        var ret: ResourceEventHandler<ActivePodSnapshot>? = null
        then {
            ret = (it.arguments[0] as ResourceEventHandler<ActivePodSnapshot>)
            answer(it)
        }
        return ret!!
    }
}
