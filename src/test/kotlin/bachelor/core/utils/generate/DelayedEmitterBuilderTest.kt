package bachelor.core.utils.generate

import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Timed
import reactor.test.StepVerifier
import java.time.Duration

class DelayedEmitterBuilderTest {


    @Test
    fun neverNoComplete() {
        val result: Flux<String> = DelayedEmitterBuilder<String>().build()
        StepVerifier.create(result)
            .verifyTimeout(Duration.ofMillis(500))
    }

    @Test
    fun neverButComplete() {
        val result: Flux<String> = DelayedEmitterBuilder<String>()
            .completeOnLast()
            .build()
        StepVerifier.create(result)
            .verifyComplete()
    }

    @Test
    fun justOnceNoComplete() {
        val result: Flux<Int> = DelayedEmitterBuilder<Int>()
            .emit(1)
            .build()
        StepVerifier.create(result)
            .expectNext(1)
            .verifyTimeout(Duration.ofMillis(500))
    }

    @Test
    fun justOnceButComplete() {
        val result: Flux<Int> = DelayedEmitterBuilder<Int>()
            .completeOnLast()
            .emit(1)
            .build()
        StepVerifier.create(result)
            .expectNext(1)
            .verifyComplete()
    }

    @Test
    fun justOnceWithDelayNoComplete() {
        val duration = Duration.ofMillis(500)
        val result: Flux<Int> = DelayedEmitterBuilder<Int>()
            .delay(duration)
            .emit(1)
            .build()
        StepVerifier.create(result.timed())
            .expectNextMatches {
                it.get() == 1 &&
                        it.elapsedSinceSubscription() >= duration
            }
            .verifyTimeout(duration + Duration.ofMillis(200))
    }


    @Test
    fun threeTimesWithDelayNoComplete() {
        val d1 = Duration.ofMillis(100)
        val d2 = Duration.ofMillis(200)
        val d3 = Duration.ofMillis(400)
        val result: Flux<Int> = DelayedEmitterBuilder<Int>()
            .emit(d1, 1, 2, 3)
            .emit(d2, 4, 5, 6)
            .delay(d3)
            .emit(7)
            .build()
        val firstPart: (Timed<Int>) -> Boolean = {
            it.get() <= 3 && it.elapsedSinceSubscription() >= d1 && it.elapsedSinceSubscription() <= d1 + d2
        }
        val secondPart: (Timed<Int>) -> Boolean = {
            4 <= it.get() && it.elapsedSinceSubscription() >= d1 + d2 && it.elapsedSinceSubscription() <= d1 + d2 + d3
        }
        val thirdPart: (Timed<Int>) -> Boolean = {
            it.get() == 7 && it.elapsedSinceSubscription() >= d1 + d2 + d3
        }

        StepVerifier.create(result.timed())
            .expectNextMatches(firstPart)
            .expectNextMatches(firstPart)
            .expectNextMatches(firstPart)

            .expectNextMatches(secondPart)
            .expectNextMatches(secondPart)
            .expectNextMatches(secondPart)
            .expectNextMatches(thirdPart)
            .verifyTimeout(d1+d2+d3+ Duration.ofMillis(200))
    }
    @Test
    fun threeTimesWithDelayAndComplete() {


        val d1 = Duration.ofMillis(100)
        val d2 = Duration.ofMillis(200)
        val d3 = Duration.ofMillis(400)
        val result: Flux<Int> = DelayedEmitterBuilder<Int>()
            .emit(d1, 1, 2, 3)
            .emit(d2, 4, 5, 6)
            .delay(d3)
            .emit(7)
            .completeOnLast()
            .build()


        val firstPart: (Timed<Int>) -> Boolean = {
            it.get() <= 3 && it.elapsedSinceSubscription() >= d1 && it.elapsedSinceSubscription() <= d1 + d2
        }
        val secondPart: (Timed<Int>) -> Boolean = {
            4 <= it.get() && it.elapsedSinceSubscription() >= d1 + d2 && it.elapsedSinceSubscription() <= d1 + d2 + d3
        }
        val thirdPart: (Timed<Int>) -> Boolean = {
            it.get() == 7 && it.elapsedSinceSubscription() >= d1 + d2 + d3
        }

        StepVerifier.create(result.timed())
            .expectNextMatches(firstPart)
            .expectNextMatches(firstPart)
            .expectNextMatches(firstPart)

            .expectNextMatches(secondPart)
            .expectNextMatches(secondPart)
            .expectNextMatches(secondPart)

            .expectNextMatches(thirdPart)
            .verifyComplete()
    }

    private fun <T> prettyPrint(build: Flux<T>) {
        build.timed()
            .doOnNext {
                println(it.elapsedSinceSubscription().toMillis().toString() + "-" + it.get())
            }.subscribe()
    }
}