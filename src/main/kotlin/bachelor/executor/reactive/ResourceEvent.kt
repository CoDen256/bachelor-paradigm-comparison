package bachelor.executor.reactive

import bachelor.core.api.snapshot.Snapshot

/**
 * [ResourceEvent] encapsulates an occurred event within a kubernetes
 * namespaces relating to some resource, like pods or jobs.
 *
 * @property action specifies, what has been done to the element
 * @property element the actual element
 */
data class ResourceEvent<out S : Snapshot>(val action: Action, val element: S?) {

    override fun toString(): String {
        return "$element[$action]"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ResourceEvent<*>) return false

        if (action != other.action) return false
        return element == other.element
    }


    override fun hashCode(): Int {
        var result = action.hashCode()
        result = 31 * result + (element?.hashCode() ?: 0)
        return result
    }
}

enum class Action {
    /** Element was added */
    ADD,

    /** Element was updated */
    UPDATE,

    /** Element was deleted */
    DELETE,

    /** No operation (for testing purposes) */
    NOOP
}
