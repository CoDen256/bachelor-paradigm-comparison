package bachelor.reactive.kubernetes.events

import bachelor.service.api.snapshot
import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.batch.v1.Job

/**
 * [ResourceEvent] encapsulates an occurred event within a kubernetes
 * namespaces relating to some resource, like pods or jobs.
 *
 * @property action specifies, what has been done to the element
 * @property element the actual element
 */
data class ResourceEvent<T : HasMetadata>(val action: Action, val element: T?) {

    override fun toString(): String {
        if (element is Pod){
            return element.snapshot(action).toString()
        }else if (element is Job){
            return element.snapshot(action).toString()
        }
        return "$action(${element?.javaClass?.simpleName})"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ResourceEvent<*>) return false

        if (action != other.action) return false
        return elementEquals(other)
    }

    private fun elementEquals(other: ResourceEvent<*>): Boolean {
        if (element is Job && other.element is Job){
            return element.snapshot(action) == other.element.snapshot(other.action)
        }
        if (element is Pod && other.element is Pod){
            return element.snapshot(action) == other.element.snapshot(other.action)
        }
        return element == null && other.element == null
    }

    override fun hashCode(): Int {
        var result = action.hashCode()
        if (element is Job){
            result = 31 * result + (element.snapshot(action).hashCode())
        }else if (element is Pod){
            result = 31 * result + (element.snapshot(action).hashCode())
        }else {
            result = 31 * result + (element?.hashCode() ?: 0)
        }
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
