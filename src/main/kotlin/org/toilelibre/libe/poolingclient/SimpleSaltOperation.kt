package org.toilelibre.libe.poolingclient

import kotlinx.coroutines.delay
import java.security.MessageDigest
import java.security.SecureRandom

object SimpleSaltOperation {

    suspend fun salt(input: String): String {
        delay(300)
        println("calling operation")
        val hash = md5(input)
        return (hash.indices).joinToString("") {
            ((hash[it].code and 0xff) + 0x100).toString(16).substring(1)
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