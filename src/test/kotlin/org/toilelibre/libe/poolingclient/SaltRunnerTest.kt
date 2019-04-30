package org.toilelibre.libe.poolingclient

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.delay
import kotlin.test.Test
import kotlin.test.assertTrue

class SaltRunnerTest {

    @Test fun `100 different inputs`() {
        val (invocationsCount, results) = SaltRunner.run { operation ->
            val results = mutableListOf<Deferred<String?>>()
            var i = 0
            repeat(100) {
                results.add(operation("text$i"))
                delay(20L)
                i++
            }

            results.toList()
        }

        assertTrue { results.size == 100 }
        assertTrue { invocationsCount == 100 }
    }

    @Test fun `same input 100 times`() {
        val (invocationsCount, results) = SaltRunner.run { operation ->
            val results = mutableListOf<Deferred<String?>>()
            repeat(100) {
                results.add(operation("sameinput"))
                delay(20L)
            }

            results.toList()
        }

        assertTrue { results.size == 100 }
        assertTrue { invocationsCount == 7 }
    }
}
