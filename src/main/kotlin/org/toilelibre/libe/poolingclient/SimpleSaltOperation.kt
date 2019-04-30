package org.toilelibre.libe.poolingclient

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.security.MessageDigest
import java.security.SecureRandom
import org.toilelibre.libe.poolingclient.PoolingClientStrategy.caller
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.awaitAll

object SimpleSaltOperation {

    suspend fun salt(input: String): String {
        delay(300)
        println("calling operation")
        val hash = md5(input)
        return (0 until hash.length).joinToString("") {
            Integer.toString(
                    (hash[it].toInt() and 0xff) + 0x100,
                    16).substring(1)
        }
    }

    private val salt = SecureRandom().run {
        val byteArray = ByteArray(16)
        this.nextBytes(byteArray)
        byteArray
    }


    private fun md5(text: String) = String(MessageDigest.getInstance("MD5")
            .apply { this.update(salt) }.run { this.digest(text.toByteArray()) })

}