package bachelor.core.api.snapshot

data class PodReference(
    val name: String,
    val namespace: String,
    val jobId: String,
) {
    override fun toString(): String {
        return "Pod($name)"
    }
}