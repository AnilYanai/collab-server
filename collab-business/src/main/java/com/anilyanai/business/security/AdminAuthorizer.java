package com.anilyanai.business.security;

/**
 * Functional Service Provider Interface (SPI) for authorizing administrative operations
 * such as closing/destroying sessions or kicking users.
 */
@FunctionalInterface
public interface AdminAuthorizer {

    /**
     * Authorizes an administrative request against a session.
     *
     * @param sessionId    the target session being administered
     * @param connectionId the physical connection ID initiating the request
     * @param adminPayload raw admin credentials / HMAC token bytes
     * @return {@code true} if caller is authorized as an administrator; {@code false} otherwise
     */
    boolean authorizeAdmin(String sessionId, String connectionId, byte[] adminPayload);

    /**
     * Permissive implementation allowing all admin operations (for local tests / dev benchmarks).
     */
    AdminAuthorizer PERMISSIVE = (sessionId, connectionId, payload) -> true;
}
