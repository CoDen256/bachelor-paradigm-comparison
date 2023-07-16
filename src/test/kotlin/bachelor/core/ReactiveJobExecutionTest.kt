package bachelor.core

import bachelor.executor.reactive.ReactiveJobExecutor

class ReactiveJobExecutionTest(): AbstractJobExecutionTest(
    { ReactiveJobExecutor(it) }
)