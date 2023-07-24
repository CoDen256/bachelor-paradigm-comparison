package bachelor.core

import bachelor.executor.imperative.ImperativeJobExecutor
import org.junit.jupiter.api.Disabled

@Disabled
class ImperativeJobExecutionIT: AbstractJobExecutionIT({ ImperativeJobExecutor(it)})
