package bachelor.core

import bachelor.executor.imperative.ImperativeJobExecutor

class ImperativeJobExecutionIT: AbstractJobExecutionIT({ ImperativeJobExecutor(it)})
