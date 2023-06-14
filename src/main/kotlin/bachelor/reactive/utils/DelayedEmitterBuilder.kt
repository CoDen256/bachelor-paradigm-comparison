package calculations.runner.utils

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * Helper class to generate elements with variable delays. It used for
 * testing purposes to create a [Flux] that emits values one by one
 * and to specify the duration of the delay between single elements.
 */
class DelayedEmitterBuilder<T> {

    private val elements = ArrayList<T>()
    private val delays = HashMap<Long, Duration>()
    private var complete = false

    /**
     * Specify whether the resulting [Flux] should complete upon emitting the
     * last element or never emit any signal
     *
     * @return
     */
    fun completeOnLast(): DelayedEmitterBuilder<T> {
        complete = true
        return this
    }

    /**
     * Emit given elements with a delay. All the elements after the first one
     * will be emitted without any delay, only the first one will be delayed,
     * thus delaying the whole sequence
     *
     * @param delay the amount to delay
     * @param elements the elements to emit
     */
    fun emit(delay: Duration, vararg elements: T): DelayedEmitterBuilder<T> {
        delay(delay)
        emit(*elements)
        return this
    }

    /** Add a delay before next elements */
    fun delay(delay: Duration): DelayedEmitterBuilder<T> {
        delays[elements.size.toLong()] = delay
        return this
    }

    /** Emit elements without a delay */
    fun emit(vararg elements: T): DelayedEmitterBuilder<T> {
        this.elements.addAll(elements)
        return this
    }

    /**
     * Build a publisher, that emits elements with variable delays. If the
     * [complete] is not set, the resulting flux won't emit a completion signal
     *
     * @return
     */
    fun build(): Flux<T> {
        var origin: Flux<T> = Flux.fromIterable(elements)
            .index()
            .delayUntil { Mono.delay(delays[it.t1] ?: Duration.ZERO) }
            .map { it.t2 }
        if (!complete) origin = origin.concatWith(Flux.never())
        return origin
    }

}