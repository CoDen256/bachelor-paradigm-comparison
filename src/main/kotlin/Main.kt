import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

private val logger: Logger = LogManager.getLogger()

fun main() {
    logger.trace("We've just greeted the user!");
    logger.debug("We've just greeted the user!");
    logger.info("We've just greeted the user!");
    logger.warn("We've just greeted the user!");
    logger.error("We've just greeted the user!");
    logger.fatal("We've just greeted the user!");
}

class Main {
}