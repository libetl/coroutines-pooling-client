package org.toilelibre.libe.poolingclient

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.security.MessageDigest
import java.security.SecureRandom
import org.toilelibre.libe.poolingclient.PoolingClientStrategy.caller
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.awaitAll

val salt = SecureRandom().run {
            val byteArray = ByteArray(16)
            this.nextBytes(byteArray)
            byteArray}


fun md5(text: String) = String(MessageDigest.getInstance("MD5")
        .apply { this.update(salt) }.run { this.digest(text.toByteArray()) })

suspend fun protect(text: String): String {
    delay(300)
    println("calling operation")
    val hash = md5(text)
    var result = ""
    for(i in 0 until hash.length)
        result += Integer.toString(
                        (hash[i].toInt() and 0xff) + 0x100,
                        16).substring(1)
    return result
}

fun main() = runBlocking {
    val caller = caller<String, String> { protect(it) }
    val results = mutableListOf<Deferred<String?>>()
    var i = 0
    repeat(100) {
        results.add(caller.callAndGroupBy("text$i"))
        delay(20L)
        //i++ // uncomment to ungroup calls
              // it should trigger more operations
    }

    println(results.awaitAll())
    println(results.size)
}
class App
