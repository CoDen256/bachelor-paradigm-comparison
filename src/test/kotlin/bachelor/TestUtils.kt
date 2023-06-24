package bachelor


import bachelor.core.impl.api.NAMESPACE
import bachelor.core.template.JobTemplateFiller
import bachelor.core.utils.generate.DelayedEmitterBuilder
import bachelor.core.utils.generate.TARGET_JOB
import org.junit.jupiter.api.Assertions
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.time.Duration


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
fun JobTemplateFiller.resolveSpec(template: String, executionTime: Long, ttl: Long, exitCode: Int = 0, fail: Boolean = false): String {
    return fill(template, mapOf(
            "NAMESPACE" to NAMESPACE,
            "NAME" to TARGET_JOB,
            "SLEEP" to "$executionTime",
            "TTL" to "$ttl",
            "CODE" to "$exitCode",
            "FAIL" to listOf("", "f/f")[fail.toInt()]
        )
    )
}

fun Boolean.toInt(): Int {
    return if (this) 1 else 0
}