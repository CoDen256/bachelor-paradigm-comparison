//package bachelor.executor.imperative
//
//import bachelor.core.api.ResourceEventListener
//import bachelor.core.api.snapshot.ActivePodSnapshot
//import bachelor.core.api.snapshot.ExecutionSnapshot
//import bachelor.core.executor.JobExecutionRequest
//import bachelor.core.executor.JobExecutor
//import bachelor.executor.reactive.ResourceEvent
//
//class ImperativeJobExecutor(private val api: Api): JobExecutor {
//    override fun execute(request: JobExecutionRequest): ExecutionSnapshot {
//        api.addPodEventListener(object : ResourceEventListener<ActivePodSnapshot>(){
//            override fun onEvent(event: ResourceEvent<ActivePodSnapshot>) {
//                TODO("Not yet implemented")
//            }
//
//        })
//
//        val reference = api.create(request.jobSpec)
//
//        // magic of listening to the events
//
//        api.removePodEventListener(null)
//        return ExecutionSnapshot(logs, podSnapshot = , jobSnapshot = )
//    }
//
//}