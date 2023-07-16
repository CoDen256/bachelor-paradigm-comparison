package bachelor.core.executor

import bachelor.core.api.prettyString
import bachelor.core.api.snapshot.ExecutionSnapshot
import java.time.Duration

/**
 * [PodNotRunningTimeoutException] is thrown, when [Pod] has not been
 * run or terminated yet within the specified deadline (has not reached
 * TerminatedState or RunningState), because it stays in WaitingState or
 * UnknownState for too long
 */
class PodNotRunningTimeoutException(val currentState: ExecutionSnapshot, val timeout: Duration) :
    ServerException("The pod main container has NOT STARTED within the specified deadline:" +
            " ${timeout.toMillis() / 1000f} s\n${prettyString(currentState)}")


/**
 * [PodNotTerminatedTimeoutException] is thrown, when [Pod] has not
 * been terminated yet withing the specified deadline (has not reached
 * TerminatedState) and it stays in WaitingState, UnknownState or
 * RunningState for too long
 */
class PodNotTerminatedTimeoutException(val currentState: ExecutionSnapshot, val timeout: Duration) :
    ClientException("The pod main container has NOT TERMINATED within the specified deadline:" +
            " ${timeout.toMillis() / 1000f} s\n${prettyString(currentState)}")


/**
 * [PodTerminatedWithErrorException] is thrown, when [Pod] has terminated
 * with a non-zero return value
 */
class PodTerminatedWithErrorException(val currentState: ExecutionSnapshot, val exitCode: Int) :
    ClientException("The pod has terminated with a NON-ZERO RETURN CODE:" +
            " $exitCode\n${prettyString(currentState)}")

