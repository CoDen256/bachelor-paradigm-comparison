package bachelor.core

import bachelor.executor.imperative.ImperativeJobExecutor

class ImperativeJobExecutorTest(): AbstractJobExecutorTest(
    { ImperativeJobExecutor(it) }
)