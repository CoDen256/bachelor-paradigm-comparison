package bachelor.executor.reactive

import bachelor.executor.AbstractJobExecutorTest

class ReactiveJobExecutorTest: AbstractJobExecutorTest(
    { ReactiveJobExecutor(it) }
)