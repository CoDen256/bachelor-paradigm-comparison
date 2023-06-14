package bachelor.service.executor

interface JobExecutor {
    fun execute(request: JobExecutionRequest): String?
}