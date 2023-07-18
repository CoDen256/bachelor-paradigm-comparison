package bachelor.core

import bachelor.core.api.snapshot.ActivePodSnapshot
import org.junit.jupiter.api.Test
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class SampleTest {
    @Test
    fun name() {
        val future = CompletableFuture<Int?>()
        future.completeAsync {
            var i = 0
            while (!future.isCancelled && !future.isCompletedExceptionally) {
                println("Sleeping: ${i++}")
                Thread.sleep(100)
            }
            if (future.isCancelled){
                println("Cancelled!!")
            } else if (future.isCompletedExceptionally) {
                println("EXCEPTIONALLY")
            }else{
                println("FINISHED")
            }
            i
        }
        println("Submitted")
        future.orTimeout(500, TimeUnit.MILLISECONDS)
        // cancel doesn't work if it already cancelled

        println("Running")
        Thread.sleep(1000)
        println(future.isCancelled)
        println("CANCELLING")
        future.cancel(true)
        println(future.isCancelled)
        try {
            println(future.get()) // cancelletion exception OR executionexception and timeoutexception
        }catch (e: Exception){
            println("EX"+e)
        }
        Thread.sleep(1000)
        println("END")
    }
}