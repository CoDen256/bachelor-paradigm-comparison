package bachelor


import bachelor.core.ImageRunRequest
import bachelor.core.JobExecutionRunner
import bachelor.core.api.snapshot.JobReference
import bachelor.core.api.snapshot.PodReference
import bachelor.core.impl.api.fabric8.reference
import bachelor.core.impl.template.BaseJobTemplateFiller
import bachelor.core.impl.template.JobTemplateFileLoader
import bachelor.core.utils.generate.DelayedEmitterBuilder
import bachelor.core.utils.generate.TARGET_JOB
import io.fabric8.kubernetes.api.model.NamespaceBuilder
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import org.awaitility.Awaitility
import org.junit.jupiter.api.Assertions
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.io.File
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference


inline fun <reified T : Throwable> Throwable.assertError(match: (T) -> Unit) {
    Assertions.assertInstanceOf(T::class.java, this)
    match(this as T)
}

inline fun <reified T : Throwable> StepVerifier.LastStep.verifyError(crossinline match: (T) -> Unit): Duration {
    return expectErrorSatisfies {
        it.assertError(match)
    }.verify()
}

fun <T> emitter(emitter: DelayedEmitterBuilder<T>.() -> DelayedEmitterBuilder<T>): Flux<T> {
    return emitter(DelayedEmitterBuilder()).build()
}

fun <T> cachedEmitter(cache: Int, builder: DelayedEmitterBuilder<T>.() -> DelayedEmitterBuilder<T>): Flux<T> {
    return emitter(builder).cache(cache)
}

fun <T> cachedEmitter(builder: DelayedEmitterBuilder<T>.() -> DelayedEmitterBuilder<T>): Flux<T> {
    return emitter(builder).cache()
}

fun millis(millis: Long): Duration = Duration.ofMillis(millis)

// GENERAL HELPER METHODS
private val loader = JobTemplateFileLoader(File(JobExecutionRunner::class.java.getResource("/template/client-test-job.yaml")!!.toURI()))

fun resolveSpec(executionTime: Long, ttl: Long, exitCode: Int = 0, fail: Boolean = false, namespace: String): String {
    return BaseJobTemplateFiller().fill(loader.getTemplate(), ImageRunRequest(
        TARGET_JOB,
        namespace,
        "busybox${if (fail) "fail" else ""}:latest",
        ttl = ttl,
        command = "/bin/sh",
        arguments = listOf(
            "-c",
            "echo start && sleep $executionTime && echo slept $executionTime && echo end && exit $exitCode" )
        )
    ).also { println(it) }
}

fun KubernetesClient.createNamespace(namespaceName: String) {
    val namespace = NamespaceBuilder()
        .withMetadata(ObjectMetaBuilder().withName(namespaceName).build()).build()
    resource(namespace).createOrReplace()
}


fun KubernetesClient.getJobs(namespace: String): List<JobReference> {
    return batch()
        .v1()
        .jobs()
        .inNamespace(namespace)
        .list()
        .items
        .map { JobReference(it.metadata.name, it.metadata.uid, it.metadata.namespace) }
}


fun KubernetesClient.getPods(namespace: String): List<PodReference> {
    return pods()
        .inNamespace(namespace)
        .list()
        .items
        .map { it.reference() }
}



const val JOB_CREATED_TIMEOUT = 5L
const val JOB_DELETED_TIMEOUT = 5L
const val JOB_DONE_TIMEOUT = 20L

const val POD_CREATED_TIMEOUT = 5L
const val POD_READY_TIMEOUT = 5L
const val POD_TERMINATED_TIMEOUT = 10L
const val POD_DELETED_TIMEOUT = 10L

// AWAITILITY HELPER METHODS
fun KubernetesClient.awaitUntilJobCreated(job: JobReference, namespace: String, timeout: Long = JOB_CREATED_TIMEOUT) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(timeout))
        .until { jobExists(job, namespace) }
}

fun KubernetesClient.awaitUntilJobDoesNotExist(job: JobReference, namespace: String, timeout: Long = JOB_DONE_TIMEOUT) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(timeout))
        .until { !jobExists(job, namespace) }
}

fun KubernetesClient.awaitUntilPodCreated(job: JobReference, namespace: String, timeout: Long = POD_CREATED_TIMEOUT): PodReference {
    val pod = AtomicReference<PodReference>()
    Awaitility.await()
        .atMost(Duration.ofSeconds(timeout))
        .until { findPod(job, namespace)?.let { pod.set(it) } != null }
    return pod.get()
}

fun KubernetesClient.awaitUntilPodReady(job: JobReference, namespace: String, timeout: Long = POD_READY_TIMEOUT) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(timeout))
        .until { podIsReady(findPod(job, namespace)!!) }
}

fun KubernetesClient.awaitUntilPodTerminated(
    job: JobReference,
    namespace: String,
    timeout: Long = POD_TERMINATED_TIMEOUT
) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(timeout))
        .until { podIsTerminated(findPod(job, namespace)!!) }
}

fun KubernetesClient.awaitNoPodsPresent(namespace: String, timeout: Long = POD_DELETED_TIMEOUT) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(timeout))
        .until { getPods(namespace).isEmpty() }
}

//  JOB AND POD REFERENCES HELPER METHODS
fun KubernetesClient.jobExists(job: JobReference, namespace: String) = getJobs(namespace).contains(job)

fun KubernetesClient.findPod(job: JobReference, namespace: String): PodReference? {
    return getPods(namespace).find { it.controllerUid == job.uid }
}


fun KubernetesClient.podIsTerminated(ref: PodReference): Boolean {
    val pod = getPod(ref)
    return pod.status.containerStatuses[0].state.terminated != null
}

fun KubernetesClient.podIsReady(ref: PodReference): Boolean {
    val pod = getPod(ref)
    return pod.status.containerStatuses[0].state.let {
        it.running != null || it.terminated != null
    }
}

private fun KubernetesClient.getPod(ref: PodReference): Pod =
    pods().inNamespace(ref.namespace).withName(ref.name).get()