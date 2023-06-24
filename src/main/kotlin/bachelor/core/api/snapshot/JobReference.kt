package bachelor.core.api.snapshot

data class JobReference(
    val name: String,
    val uid: String,
    val namespace: String,
) {
    override fun toString(): String {
        return "Job($name,$uid)"
    }
}