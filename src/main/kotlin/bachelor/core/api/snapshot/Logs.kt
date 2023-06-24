package bachelor.core.api.snapshot

import org.apache.commons.text.StringEscapeUtils

/**
 * [Logs] represents a wrapper object around logs that are produced by
 * container in a pod
 */
data class Logs(val content: String?) {
    constructor(bytes: ByteArray) : this(String(bytes))

    override fun toString(): String {
        return StringEscapeUtils.escapeJava(getContentOrEmpty())
    }

    fun getContentOrEmpty(): String {
        return content.orEmpty()
    }

    companion object {
        fun empty(): Logs {
            return Logs(null)
        }
    }
}