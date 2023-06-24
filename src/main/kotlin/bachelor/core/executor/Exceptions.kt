package bachelor.core.executor

/**
 * Represents an exception based on the client error or script execution.
 * Associated with an error during execution of the image script itself or
 * an execution timeout.
 */
open class ClientException(override val message: String) : RuntimeException(message)

/**
 * Represents a server exception. Associated with unexpected errors
 * occurring during the execution of the image.
 */
open class ServerException(override val message: String, override val cause: Throwable? = null) :
    RuntimeException(message, cause)
