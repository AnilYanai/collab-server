package com.anilyanai.collabclient.security

import java.nio.charset.StandardCharsets
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * Generates cryptographically signed HMAC-SHA256 room tokens on Android.
 */
object HmacTokenAuthorizer {

    private const val HMAC_ALGORITHM = "HmacSHA256"

    fun generateToken(
        sessionId: String,
        userId: String,
        durationSeconds: Long,
        secretKey: String
    ): String {
        val expiryEpochSec = (System.currentTimeMillis() / 1000L) + durationSeconds
        val claims = "$sessionId:$userId:$expiryEpochSec"

        val mac = Mac.getInstance(HMAC_ALGORITHM)
        mac.init(SecretKeySpec(secretKey.toByteArray(StandardCharsets.UTF_8), HMAC_ALGORITHM))
        val signature = mac.doFinal(claims.toByteArray(StandardCharsets.UTF_8))

        val encodedSig = Base64.getUrlEncoder().withoutPadding().encodeToString(signature)
        return "$claims.$encodedSig"
    }
}
