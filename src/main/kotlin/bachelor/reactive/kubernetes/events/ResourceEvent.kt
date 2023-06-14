package bachelor.reactive.kubernetes.events

import io.fabric8.kubernetes.api.model.HasMetadata

/**
 * [ResourceEvent] encapsulates an occurred event within a kubernetes
 * namespaces relating to some resource, like pods or jobs.
 *
 * @property action specifies, what has been done to the element
 * @property element the actual element
 */
data class ResourceEvent<T : HasMetadata>(val action: Action, val element: T?) {

    override fun toString(): String {
        val element = element?.let { "${it::class.simpleName}/${it.metadata.name}" } ?: "<N/A>"
        return "$action($element)"
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
