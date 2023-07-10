package bachelor.core

import bachelor.executor.reactive.ReactiveJobExecutor

class ReactiveJobExecutionIT: AbstractJobExecutionIT({ ReactiveJobExecutor(it)})
