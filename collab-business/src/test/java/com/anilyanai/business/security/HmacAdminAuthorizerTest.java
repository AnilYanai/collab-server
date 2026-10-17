package com.anilyanai.business.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HmacAdminAuthorizerTest {

    private static final String ADMIN_SECRET = "super-secret-admin-master-key-32-chars";
    private HmacAdminAuthorizer authorizer;

    @BeforeEach
    void setUp() {
        authorizer = new HmacAdminAuthorizer(ADMIN_SECRET);
    }

    @Test
    void testValidAdminTokenAuthorizesSuccessfully() {
        String token = HmacAdminAuthorizer.generateAdminToken("room-101", "root-admin", 60, ADMIN_SECRET);
        byte[] payload = token.getBytes(StandardCharsets.UTF_8);

        boolean authorized = authorizer.authorizeAdmin("room-101", "conn-admin", payload);
        assertTrue(authorized);
    }

    @Test
    void testAdminTokenForDifferentSessionFails() {
        String token = HmacAdminAuthorizer.generateAdminToken("room-101", "root-admin", 60, ADMIN_SECRET);
        byte[] payload = token.getBytes(StandardCharsets.UTF_8);

        boolean authorized = authorizer.authorizeAdmin("room-202", "conn-admin", payload);
        assertFalse(authorized);
    }

    @Test
    void testTamperedAdminTokenFails() {
        String token = HmacAdminAuthorizer.generateAdminToken("room-101", "root-admin", 60, ADMIN_SECRET);
        String tampered = token.replace("ADMIN", "USER");
        byte[] payload = tampered.getBytes(StandardCharsets.UTF_8);

        boolean authorized = authorizer.authorizeAdmin("room-101", "conn-admin", payload);
        assertFalse(authorized);
    }

    @Test
    void testExpiredAdminTokenFails() {
        // Expired 10 seconds ago (-10 duration)
        String token = HmacAdminAuthorizer.generateAdminToken("room-101", "root-admin", -10, ADMIN_SECRET);
        byte[] payload = token.getBytes(StandardCharsets.UTF_8);

        boolean authorized = authorizer.authorizeAdmin("room-101", "conn-admin", payload);
        assertFalse(authorized);
    }
}
