package bachelor.executor.imperative

import bachelor.executor.AbstractJobExecutorIT

class ImperativeJobExecutorIT: AbstractJobExecutorIT({ ImperativeJobExecutor(it)})
