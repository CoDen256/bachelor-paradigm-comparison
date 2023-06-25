package bachelor


import bachelor.core.ImageRunRequest
import bachelor.core.JobExecutionRunner
import bachelor.core.impl.api.NAMESPACE
import bachelor.core.impl.template.BaseJobTemplateFiller
import bachelor.core.impl.template.JobTemplateFileLoader
import bachelor.core.utils.generate.DelayedEmitterBuilder
import bachelor.core.utils.generate.TARGET_JOB
import org.junit.jupiter.api.Assertions
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.io.File
import java.time.Duration
import javax.swing.text.html.HTML.Attribute.CODE


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

fun resolveSpec(executionTime: Long, ttl: Long, exitCode: Int = 0, fail: Boolean = false): String {
    return BaseJobTemplateFiller().fill(loader.getTemplate(), ImageRunRequest(
        TARGET_JOB,
        NAMESPACE,
        "busybox${if (fail) "fail" else ""}:latest",
        ttl = ttl,
        command = "/bin/sh",
        arguments = listOf(
            "-c",
            "echo start && sleep $executionTime && echo slept $executionTime && echo end && exit $exitCode" )
        )
    ).also { println(it) }
}

fun Boolean.toInt(): Int {
    return if (this) 1 else 0
}