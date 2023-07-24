package bachelor.executor.imperative

import bachelor.executor.AbstractJobExecutorTest

class ImperativeJobExecutorTest(): AbstractJobExecutorTest(
    { ImperativeJobExecutor(it) }
)