package bachelor.core

import bachelor.executor.imperative.ImperativeJobExecutor

class ImperativeJobExecutionTest(): AbstractJobExecutionTest(
    { ImperativeJobExecutor(it) }
)