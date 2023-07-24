package bachelor.executor.reactive

import bachelor.executor.AbstractJobExecutorIT

class ReactiveJobExecutorIT: AbstractJobExecutorIT({ ReactiveJobExecutor(it)})
