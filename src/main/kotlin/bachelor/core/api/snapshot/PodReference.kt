package bachelor.core.api.snapshot

data class PodReference(
    val name: String,
    val uid: String,
    val namespace: String,
    val controllerUid: String,
) {
    override fun toString(): String {
        return "Pod($name)"
    }
}