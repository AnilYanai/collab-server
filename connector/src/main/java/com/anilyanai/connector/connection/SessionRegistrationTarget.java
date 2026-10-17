package com.anilyanai.connector.connection;

/**
 * Double-dispatch registration target for Sessions.
 * Enables zero-instanceof, branchless registration of typed connections.
 */
public interface SessionRegistrationTarget {

    void registerUDP(UDPConnection connection);

    void registerTCP(TCPConnection connection);

    void unregisterUDP(String connectionId);

    void unregisterTCP(String connectionId);
}
