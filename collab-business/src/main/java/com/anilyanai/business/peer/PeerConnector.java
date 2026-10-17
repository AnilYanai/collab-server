package com.anilyanai.business.peer;

/**
 * Functional abstraction for establishing an outbound connection to a cluster peer node.
 */
@FunctionalInterface
public interface PeerConnector {

    /**
     * Dials and establishes connection to a remote cluster peer host.
     *
     * @param peerAddress host:port of the peer node (e.g. "192.168.1.50:9090")
     * @return registered connection ID for the peer, or null if connection failed
     */
    String connectToPeer(String peerAddress);
}
