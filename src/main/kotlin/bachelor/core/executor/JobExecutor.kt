package bachelor.core.executor

import bachelor.core.api.snapshot.ExecutionSnapshot

interface JobExecutor {
    fun execute(request: JobExecutionRequest): ExecutionSnapshot
}