package com.anilyanai.business.security;

import lombok.extern.slf4j.Slf4j;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;

/**
 * =========================================================================================
 * 🔐 HmacAdminAuthorizer (Stateless Master-Admin Cryptographic Authorizer)
 * =========================================================================================
 */
@Slf4j
public class HmacAdminAuthorizer implements AdminAuthorizer {

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private final byte[] adminSecretBytes;

    public HmacAdminAuthorizer(String adminSecret) {
        if (adminSecret == null || adminSecret.isBlank()) {
            throw new IllegalArgumentException("Admin secret key cannot be null or blank");
        }
        this.adminSecretBytes = adminSecret.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean authorizeAdmin(String sessionId, String connectionId, byte[] adminPayload) {
        if (adminPayload == null || adminPayload.length == 0) {
            log.warn("Unauthorized admin operation: Empty admin payload for session {}", sessionId);
            return false;
        }

        String tokenString = new String(adminPayload, StandardCharsets.UTF_8).trim();
        int dotIndex = tokenString.lastIndexOf('.');
        if (dotIndex <= 0 || dotIndex == tokenString.length() - 1) {
            log.warn("Unauthorized admin operation: Malformed token structure for session {}", sessionId);
            return false;
        }

        String claimsPart = tokenString.substring(0, dotIndex);
        String signaturePart = tokenString.substring(dotIndex + 1);

        String[] parts = claimsPart.split(":");
        if (parts.length < 4) {
            log.warn("Unauthorized admin operation: Insufficient admin claims for session {}", sessionId);
            return false;
        }

        String tokenSessionId = parts[0];
        String tokenAdminId = parts[1];
        String tokenRole = parts[2];
        String expiryStr = parts[3];

        // 1. Enforce target session match
        if (!sessionId.equals(tokenSessionId)) {
            log.warn("Unauthorized admin operation: Token session '{}' does not match target session '{}'",
                    tokenSessionId, sessionId);
            return false;
        }

        // 2. Enforce ADMIN role claim
        if (!"ADMIN".equalsIgnoreCase(tokenRole)) {
            log.warn("Unauthorized admin operation: Token role '{}' is not ADMIN for session {}",
                    tokenRole, sessionId);
            return false;
        }

        // 3. Enforce expiration window
        try {
            long expiryEpochSec = Long.parseLong(expiryStr);
            long currentEpochSec = Instant.now().getEpochSecond();
            if (currentEpochSec > expiryEpochSec) {
                log.warn("Unauthorized admin operation: Expired admin token for session {} (expired at {}, current {})",
                        sessionId, expiryEpochSec, currentEpochSec);
                return false;
            }
        } catch (NumberFormatException e) {
            log.warn("Unauthorized admin operation: Invalid expiry timestamp '{}' for session {}", expiryStr, sessionId);
            return false;
        }

        // 4. Verify HMAC-SHA256 signature using Master Admin Secret
        try {
            Mac hmac = Mac.getInstance(HMAC_ALGORITHM);
            SecretKeySpec secretKey = new SecretKeySpec(adminSecretBytes, HMAC_ALGORITHM);
            hmac.init(secretKey);

            byte[] expectedSig = hmac.doFinal(claimsPart.getBytes(StandardCharsets.UTF_8));
            byte[] providedSig = Base64.getUrlDecoder().decode(signaturePart);

            if (!MessageDigest.isEqual(expectedSig, providedSig)) {
                log.warn("Unauthorized admin operation: Invalid cryptographic signature for session {}", sessionId);
                return false;
            }

            log.info("Admin operation authorized for session {} by admin {}", sessionId, tokenAdminId);
            return true;

        } catch (NoSuchAlgorithmException | InvalidKeyException | IllegalArgumentException e) {
            log.error("Error verifying admin token signature for session {}: {}", sessionId, e.getMessage());
            return false;
        }
    }

    public static String generateAdminToken(String sessionId, String adminId, long durationSeconds, String adminSecret) {
        long expiryEpochSec = Instant.now().getEpochSecond() + durationSeconds;
        String claims = sessionId + ":" + adminId + ":ADMIN:" + expiryEpochSec;

        try {
            Mac hmac = Mac.getInstance(HMAC_ALGORITHM);
            SecretKeySpec secretKey = new SecretKeySpec(adminSecret.getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM);
            hmac.init(secretKey);

            byte[] signature = hmac.doFinal(claims.getBytes(StandardCharsets.UTF_8));
            String base64Sig = Base64.getUrlEncoder().withoutPadding().encodeToString(signature);

            return claims + "." + base64Sig;
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException("Failed to generate admin HMAC token", e);
        }
    }
}
