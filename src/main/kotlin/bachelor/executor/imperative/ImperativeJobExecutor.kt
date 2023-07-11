package bachelor.executor.imperative

import bachelor.core.api.JobApi
import bachelor.core.api.ResourceEventHandler
import bachelor.core.api.isPodRunningOrTerminated
import bachelor.core.api.snapshot.*
import bachelor.core.executor.JobExecutionRequest
import bachelor.core.executor.JobExecutor
import bachelor.core.executor.PodNotRunningTimeoutException
import java.time.Duration
import java.util.ArrayList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class ImperativeJobExecutor(private val api: JobApi): JobExecutor {
    override fun execute(request: JobExecutionRequest): ExecutionSnapshot {
        var job: JobReference? = null
        val podEvents = ArrayList<PodSnapshot>().apply { add(InitialPodSnapshot) }
        val jobEvents = ArrayList<JobSnapshot>().apply { add(InitialJobSnapshot) }

        val podHandler = ResourceEventHandler<ActivePodSnapshot> { event -> event.element?.let { podEvents.add(it) } }
        val jobHandler = ResourceEventHandler<ActiveJobSnapshot> { event -> event.element?.let { jobEvents.add(it) } }
        try {
            api.addPodEventHandler(podHandler)
            api.addJobEventHandler(jobHandler)



            job = api.create(request.jobSpec)


            waitUntilPodRunningOrTerminated(podEvents, jobEvents, request.isRunningTimeout)
            waitUntilPodTerminated(podEvents, jobEvents, request.isTerminatedTimeout)

            verifyTermination(podEvents, jobEvents)



            val snapshot = ExecutionSnapshot(
                Logs.empty(),
                jobEvents.last(),
                podEvents.last(),
            )
            return snapshot
        }finally {
            job?.let { api.delete(it) }
            api.removePodEventHandler(podHandler)
            api.removeJobEventHandler(jobHandler)
        }
    }

    private fun waitUntilPodRunningOrTerminated(
        podEvents: List<PodSnapshot>,
        jobEvents: List<JobSnapshot>,
        runningTimeout: Duration
    ) {
        Executors.newSingleThreadExecutor().submit {
            while (podEvents.none { isPodRunningOrTerminated(it) }){ }
        }.get(runningTimeout.toMillis(), TimeUnit.MILLISECONDS)
    }

    private fun waitUntilPodTerminated(
        podEvents: ArrayList<PodSnapshot>,
        jobEvents: ArrayList<JobSnapshot>,
        terminatedTimeout: Duration
    ) {
        try {

            Executors.newSingleThreadExecutor().submit {
                while (podEvents.none { isPodRunningOrTerminated(it) }){ }
            }.get(terminatedTimeout.toMillis(), TimeUnit.MILLISECONDS)
        }catch (e: TimeoutException){
            throw PodNotRunningTimeoutException(null!!, null!!)
        }
    }

    private fun verifyTermination(podEvents: ArrayList<PodSnapshot>, jobEvents: ArrayList<JobSnapshot>) {
        TODO("Not yet implemented")
    }

}