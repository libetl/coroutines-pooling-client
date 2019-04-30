package org.toilelibre.libe.poolingclient

import kotlinx.coroutines.*
import org.toilelibre.libe.poolingclient.PoolingClientStrategy.caller

object SaltRunner {

    inline fun run(crossinline block: SaltOrchestration) = runBlocking {
        val caller = caller<String, String> { SimpleSaltOperation.salt(it) }
        val results = block(this) { caller.callAndGroupBy(it) }

        println(results.awaitAll())
        println(results.size)

        caller.stop()

        caller.invocationsCount() to results
    }
}
