package bachelor.service.executor

import bachelor.service.api.snapshot.ExecutionSnapshot

interface JobExecutor {
    fun execute(request: JobExecutionRequest): ExecutionSnapshot
}