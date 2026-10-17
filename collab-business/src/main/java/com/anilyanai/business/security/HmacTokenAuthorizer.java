package com.anilyanai.business.security;

import lombok.extern.slf4j.Slf4j;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * =========================================================================================
 * 🛡️ HmacTokenAuthorizer (High-Speed Cryptographic Token Verifier)
 * =========================================================================================
 *
 * <p>Validates stateless, tamper-proof session tokens using HMAC-SHA256.</p>
 */
@Slf4j
public class HmacTokenAuthorizer implements SessionAuthorizer {

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private final byte[] secretKeyBytes;

    public HmacTokenAuthorizer(String secretKey) {
        if (secretKey == null || secretKey.isBlank()) {
            throw new IllegalArgumentException("HMAC secret key cannot be null or blank");
        }
        this.secretKeyBytes = secretKey.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean authorize(String sessionId, String connectionId, byte[] authPayload) {
        return authorizeWithResult(sessionId, connectionId, authPayload).authorized();
    }

    @Override
    public AuthResult authorizeWithResult(String sessionId, String connectionId, byte[] authPayload) {
        if (authPayload == null || authPayload.length == 0) {
            log.warn("Unauthorized join: Empty auth payload for session {}", sessionId);
            return AuthResult.REJECTED;
        }

        String token = new String(authPayload, StandardCharsets.UTF_8).trim();
        int dotIndex = token.lastIndexOf('.');
        if (dotIndex == -1) {
            log.warn("Unauthorized join: Invalid token format for session {}", sessionId);
            return AuthResult.REJECTED;
        }

        String claims = token.substring(0, dotIndex);
        String providedSignatureBase64 = token.substring(dotIndex + 1);

        String[] parts = claims.split(":", 4);
        if (parts.length < 3) {
            log.warn("Unauthorized join: Malformed token claims for session {}", sessionId);
            return AuthResult.REJECTED;
        }

        String tokenSessionId = parts[0];
        long expiryEpochSec;
        try {
            expiryEpochSec = Long.parseLong(parts[2]);
        } catch (NumberFormatException e) {
            log.warn("Unauthorized join: Invalid token expiration timestamp");
            return AuthResult.REJECTED;
        }

        String hostServer = (parts.length >= 4 && !parts[3].isBlank()) ? parts[3].trim() : null;

        // 1. Enforce session room match
        if (!sessionId.equals(tokenSessionId)) {
            log.warn("Unauthorized join: Token session '{}' does not match target session '{}'",
                    tokenSessionId, sessionId);
            return AuthResult.REJECTED;
        }

        // 2. Enforce expiration timestamp
        long currentEpochSec = System.currentTimeMillis() / 1000L;
        if (currentEpochSec > expiryEpochSec) {
            log.warn("Unauthorized join: Expired token for session {} (expired at {}, current {})",
                    sessionId, expiryEpochSec, currentEpochSec);
            return AuthResult.REJECTED;
        }

        // 3. Cryptographic signature check
        try {
            byte[] expectedSignature = computeHmac(claims.getBytes(StandardCharsets.UTF_8));
            byte[] providedSignature = Base64.getUrlDecoder().decode(providedSignatureBase64);

            boolean valid = MessageDigest.isEqual(expectedSignature, providedSignature);
            if (!valid) {
                log.warn("Unauthorized join: Invalid cryptographic signature for session {}", sessionId);
                return AuthResult.REJECTED;
            }
            return AuthResult.approved(hostServer);
        } catch (Exception e) {
            log.error("Error verifying HMAC token for session {}", sessionId, e);
            return AuthResult.REJECTED;
        }
    }

    private byte[] computeHmac(byte[] data) throws Exception {
        Mac mac = Mac.getInstance(HMAC_ALGORITHM);
        mac.init(new SecretKeySpec(secretKeyBytes, HMAC_ALGORITHM));
        return mac.doFinal(data);
    }

    public static String generateToken(String sessionId, String userId, long durationSeconds, String secretKey) {
        return generateToken(sessionId, userId, durationSeconds, null, secretKey);
    }

    public static String generateToken(String sessionId, String userId, long durationSeconds, String hostServer, String secretKey) {
        try {
            long expiryEpochSec = (System.currentTimeMillis() / 1000L) + durationSeconds;
            String claims = (hostServer != null && !hostServer.isBlank())
                    ? sessionId + ":" + userId + ":" + expiryEpochSec + ":" + hostServer.trim()
                    : sessionId + ":" + userId + ":" + expiryEpochSec;

            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM));
            byte[] signature = mac.doFinal(claims.getBytes(StandardCharsets.UTF_8));

            return claims + "." + Base64.getUrlEncoder().withoutPadding().encodeToString(signature);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate HMAC token", e);
        }
    }
}
