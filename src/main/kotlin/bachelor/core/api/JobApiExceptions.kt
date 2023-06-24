package bachelor.core.api

import bachelor.core.executor.ServerException

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

