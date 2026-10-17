package com.anilyanai.business.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class HmacTokenAuthorizerTest {

    private static final String SECRET_KEY = "super-secret-cluster-token-key-32-bytes";
    private HmacTokenAuthorizer authorizer;

    @BeforeEach
    void setUp() {
        authorizer = new HmacTokenAuthorizer(SECRET_KEY);
    }

    @Test
    void testValidTokenAuthorizesSuccessfully() {
        String token = HmacTokenAuthorizer.generateToken("room-101", "user-1", 60, SECRET_KEY);
        byte[] payload = token.getBytes(StandardCharsets.UTF_8);

        AuthResult result = authorizer.authorizeWithResult("room-101", "conn-1", payload);

        assertTrue(result.authorized());
        assertNull(result.hostServer());
        assertTrue(authorizer.authorize("room-101", "conn-1", payload));
    }

    @Test
    void testValidTokenWithHostServerExtractsHost() {
        String token = HmacTokenAuthorizer.generateToken("room-101", "user-1", 60, "10.0.0.5:9090", SECRET_KEY);
        byte[] payload = token.getBytes(StandardCharsets.UTF_8);

        AuthResult result = authorizer.authorizeWithResult("room-101", "conn-1", payload);

        assertTrue(result.authorized());
        assertEquals("10.0.0.5:9090", result.hostServer());
    }

    @Test
    void testSessionIdMismatchFails() {
        String token = HmacTokenAuthorizer.generateToken("room-101", "user-1", 60, SECRET_KEY);
        byte[] payload = token.getBytes(StandardCharsets.UTF_8);

        // Attempting to use room-101's token to access room-999
        AuthResult result = authorizer.authorizeWithResult("room-999", "conn-1", payload);

        assertFalse(result.authorized());
    }

    @Test
    void testTamperedPayloadFails() {
        String token = HmacTokenAuthorizer.generateToken("room-101", "user-1", 60, SECRET_KEY);
        String tampered = token.replace("user-1", "user-admin");
        byte[] payload = tampered.getBytes(StandardCharsets.UTF_8);

        AuthResult result = authorizer.authorizeWithResult("room-101", "conn-1", payload);

        assertFalse(result.authorized());
    }

    @Test
    void testEmptyPayloadFails() {
        AuthResult result = authorizer.authorizeWithResult("room-101", "conn-1", new byte[0]);
        assertFalse(result.authorized());

        AuthResult resultNull = authorizer.authorizeWithResult("room-101", "conn-1", null);
        assertFalse(resultNull.authorized());
    }
}
