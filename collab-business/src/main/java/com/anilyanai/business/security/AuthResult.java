package com.anilyanai.business.security;

/**
 * Result of session authorization containing approval status and optional routing metadata.
 *
 * @param authorized whether the client is permitted to join
 * @param hostServer optional host server address extracted from the token (e.g., "192.168.1.50:9090")
 */
public record AuthResult(boolean authorized, String hostServer) {

    public static final AuthResult REJECTED = new AuthResult(false, null);
    public static final AuthResult APPROVED_LOCAL = new AuthResult(true, null);

    public static AuthResult approved(String hostServer) {
        return new AuthResult(true, hostServer);
    }
}
