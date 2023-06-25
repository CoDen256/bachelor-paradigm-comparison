package bachelor.core.api.snapshot

data class PodReference(
    val name: String,
    val uid: String,
    val controllerUid: String,
    val namespace: String,
) {
    override fun toString(): String {
        return "Pod($name)"
    }
}