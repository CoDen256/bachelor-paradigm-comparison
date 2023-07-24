package bachelor.executor.reactive

import bachelor.executor.AbstractJobExecutorTest
import org.junit.jupiter.api.Disabled

class ReactiveJobExecutorTest: AbstractJobExecutorTest(
    { ReactiveJobExecutor(it) }
)