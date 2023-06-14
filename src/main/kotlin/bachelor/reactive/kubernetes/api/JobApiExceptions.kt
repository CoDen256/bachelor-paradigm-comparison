package bachelor.reactive.kubernetes.api

import bachelor.reactive.kubernetes.api.snapshot.ExecutionSnapshot
import bachelor.service.run.ClientException
import bachelor.service.run.ServerException
import calculations.runner.kubernetes.api.prettyString
import java.time.Duration

/**
 * [InvalidJobSpecException] is thrown, when the job spec created from
 * template has invalid syntax and cannot be loaded to a job
 */
class InvalidJobSpecException(msg: String, cause: Throwable?) : ServerException(msg, cause)


/**
 * [JobAlreadyExistsException] is thrown, when [JobApi] attempts to create
 * a job that is already running the Cluster
 */
class JobAlreadyExistsException(msg: String, cause: Throwable?) : ServerException(msg, cause)


/**
 * [PodNotRunningTimeoutException] is thrown, when [Pod] has not been
 * run or terminated yet within the specified deadline (has not reached
 * TerminatedState or RunningState), because it stays in WaitingState or
 * UnknownState for too long
 */
class PodNotRunningTimeoutException(val currentState: ExecutionSnapshot, timeout: Duration) :
    ServerException("The pod main container has NOT STARTED within the specified deadline:" +
            " ${timeout.toMillis() / 1000f} s\n${prettyString(currentState)}")


/**
 * [PodNotTerminatedTimeoutException] is thrown, when [Pod] has not
 * been terminated yet withing the specified deadline (has not reached
 * TerminatedState) and it stays in WaitingState, UnknownState or
 * RunningState for too long
 */
class PodNotTerminatedTimeoutException(val currentState: ExecutionSnapshot, timeout: Duration) :
    ClientException("The pod main container has NOT TERMINATED within the specified deadline:" +
            " ${timeout.toMillis() / 1000f} s\n${prettyString(currentState)}")


/**
 * [PodTerminatedWithErrorException] is thrown, when [Pod] has terminated
 * with a non-zero return value
 */
class PodTerminatedWithErrorException(val currentState: ExecutionSnapshot, exitCode: Int) :
    ClientException("The pod has terminated with a NON-ZERO RETURN CODE:" +
            " $exitCode\n${prettyString(currentState)}")
