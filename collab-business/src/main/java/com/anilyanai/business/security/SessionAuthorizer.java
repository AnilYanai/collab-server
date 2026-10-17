package com.anilyanai.business.security;

/**
 * Functional interface for authenticating and authorizing client session access.
 */
@FunctionalInterface
public interface SessionAuthorizer {

    /**
     * Validates whether a client is authorized to join the designated session.
     *
     * @param sessionId    the target session/room identifier
     * @param connectionId physical connection identifier of the client
     * @param authPayload  raw credentials or token sent inside the JOIN_SESSION packet payload
     * @return {@code true} if access is granted; {@code false} if unauthorized
     */
    boolean authorize(String sessionId, String connectionId, byte[] authPayload);

    /**
     * Authorizes the client and extracts routing metadata (e.g. host server from JWT/token).
     */
    default AuthResult authorizeWithResult(String sessionId, String connectionId, byte[] authPayload) {
        boolean ok = authorize(sessionId, connectionId, authPayload);
        return ok ? AuthResult.APPROVED_LOCAL : AuthResult.REJECTED;
    }

    /**
     * Permissive default authorizer for development, benchmarking, or open rooms.
     */
    SessionAuthorizer PERMISSIVE = (sessionId, connectionId, authPayload) -> true;
}
