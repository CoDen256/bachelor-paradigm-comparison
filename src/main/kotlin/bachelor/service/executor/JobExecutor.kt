package bachelor.service.executor

import bachelor.service.executor.snapshot.ExecutionSnapshot

interface JobExecutor {
    fun execute(request: JobExecutionRequest): ExecutionSnapshot
}