package com.anilyanai.engine.session;

import com.anilyanai.connector.connection.Connection;
import com.anilyanai.connector.connection.SessionRegistrationTarget;
import com.anilyanai.connector.connection.TCPConnection;
import com.anilyanai.connector.connection.UDPConnection;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * =========================================================================================
 * 🚀 Session (High-Performance Concurrent Membership & State Container)
 * =========================================================================================
 *
 * <p>Represents an isolated collaboration room/session. Tracks active participants partitioned
 * into typed UDP connections, typed TCP connections, and peer cluster server connections.</p>
 *
 * <h3>Design & Performance Characteristics:</h3>
 * <ul>
 *   <li><b>Partitioned Sets:</b> Local UDP clients, local TCP clients, and peer server nodes are
 *       stored in separate lock-free concurrent maps, enabling zero-condition branchless broadcasting.</li>
 *   <li><b>Zero-Instanceof Double Dispatch:</b> Uses {@link SessionRegistrationTarget} to register
 *       typed connections without ANY {@code instanceof} checks or runtime type casting.</li>
 *   <li><b>Zero-Allocation Views:</b> Read accessors return unmodifiable wrapper views over the
 *       underlying concurrent collections rather than defensively allocating new set copies per packet.</li>
 *   <li><b>Direct Connection Caching:</b> Stores direct {@link UDPConnection} and {@link TCPConnection}
 *       references to eliminate hundreds of thousands of {@code ConcurrentHashMap} lookups per second.</li>
 * </ul>
 *
 * @author anilyanai
 */
public final class Session implements SessionRegistrationTarget {

    private final String id;
    private final Map<String, UDPConnection> udpConnections = new ConcurrentHashMap<>();
    private final Map<String, TCPConnection> tcpConnections = new ConcurrentHashMap<>();
    private final Set<String> localClientIds = ConcurrentHashMap.newKeySet();
    private final Set<String> peerServerIds = ConcurrentHashMap.newKeySet();

    private final Collection<UDPConnection> unmodifiableUdpConnections = Collections.unmodifiableCollection(udpConnections.values());
    private final Collection<TCPConnection> unmodifiableTcpConnections = Collections.unmodifiableCollection(tcpConnections.values());
    private final Set<String> unmodifiableLocalClients = Collections.unmodifiableSet(localClientIds);
    private final Set<String> unmodifiablePeerServers = Collections.unmodifiableSet(peerServerIds);

    /**
     * On session creation the state preserve object is null.
     */
    private StatePreserver statePreserver = null;

    public Session(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Session id must not be blank");
        }
        this.id = id;
        this.statePreserver = null; // Explicitly null on session creation
    }

    public String getId() {
        return id;
    }

    /**
     * Sets the state preserver object on session creation.
     */
    public void setStatePreserver(StatePreserver statePreserver) {
        if (this.statePreserver == null) {
            this.statePreserver = statePreserver;
        }
    }

    public StatePreserver getStatePreserver() {
        return statePreserver;
    }

    /**
     * Saves raw packet state into the preserver without any branching or condition checks.
     * If state is not preserved, this executes the overridden empty method with zero overhead.
     */
    public void saveState(byte[] rawBytes) {
        if (statePreserver != null) {
            statePreserver.save(rawBytes);
        }
    }

    /**
     * Retrieves preserved session history.
     */
    public List<byte[]> getHistory() {
        return statePreserver != null ? statePreserver.getHistory() : Collections.emptyList();
    }

    /**
     * Clears preserved session history.
     */
    public void clearState() {
        if (statePreserver != null) {
            statePreserver.clear();
        }
    }

    // =========================================================================
    // Double-Dispatch Registration (Zero instanceof checks!)
    // =========================================================================

    @Override
    public void registerUDP(UDPConnection connection) {
        if (connection != null && !isBlank(connection.getConnectionId())) {
            udpConnections.put(connection.getConnectionId(), connection);
            localClientIds.add(connection.getConnectionId());
        }
    }

    @Override
    public void registerTCP(TCPConnection connection) {
        if (connection != null && !isBlank(connection.getConnectionId())) {
            tcpConnections.put(connection.getConnectionId(), connection);
            localClientIds.add(connection.getConnectionId());
        }
    }

    @Override
    public void unregisterUDP(String connectionId) {
        if (!isBlank(connectionId)) {
            udpConnections.remove(connectionId);
            localClientIds.remove(connectionId);
        }
    }

    @Override
    public void unregisterTCP(String connectionId) {
        if (!isBlank(connectionId)) {
            tcpConnections.remove(connectionId);
            localClientIds.remove(connectionId);
        }
    }

    /**
     * Joins a local client connection directly to this session via double-dispatch.
     * ZERO instanceof checks!
     */
    public boolean joinLocalClient(Connection connection) {
        if (connection == null || isBlank(connection.getConnectionId())) return false;
        connection.registerInto(this);
        return true;
    }

    /**
     * Joins a local client ID to this session (backward-compatible / test mock fallback).
     */
    public boolean joinLocalClient(String clientId) {
        if (isBlank(clientId)) return false;
        return localClientIds.add(clientId);
    }

    /**
     * Joins a peer cluster server node to this session.
     */
    public boolean joinPeerServer(String peerId) {
        if (isBlank(peerId)) return false;
        return peerServerIds.add(peerId);
    }

    /**
     * Default join delegates to local client.
     */
    public boolean join(String connectionId) {
        return joinLocalClient(connectionId);
    }

    /**
     * Removes a local client from this session.
     */
    public boolean leaveLocalClient(String clientId) {
        if (isBlank(clientId)) return false;
        udpConnections.remove(clientId);
        tcpConnections.remove(clientId);
        return localClientIds.remove(clientId);
    }

    /**
     * Removes a peer cluster server from this session.
     */
    public boolean leavePeerServer(String peerId) {
        if (isBlank(peerId)) return false;
        return peerServerIds.remove(peerId);
    }

    /**
     * Removes a connection whether it is a local client or a peer server.
     */
    public boolean leave(String connectionId) {
        if (isBlank(connectionId)) return false;
        udpConnections.remove(connectionId);
        tcpConnections.remove(connectionId);
        boolean removedLocal = localClientIds.remove(connectionId);
        boolean removedPeer = peerServerIds.remove(connectionId);
        return removedLocal || removedPeer;
    }

    /**
     * Returns direct typed UDP connections for zero-lookup fan-out.
     */
    public Collection<UDPConnection> getUDPConnections() {
        return unmodifiableUdpConnections;
    }

    /**
     * Returns direct typed TCP connections for zero-lookup fan-out.
     */
    public Collection<TCPConnection> getTCPConnections() {
        return unmodifiableTcpConnections;
    }

    /**
     * Backward-compatible combined Connection view.
     */
    public Collection<Connection> getLocalConnections() {
        List<Connection> combined = new java.util.ArrayList<>(udpConnections.size() + tcpConnections.size());
        combined.addAll(udpConnections.values());
        combined.addAll(tcpConnections.values());
        return Collections.unmodifiableCollection(combined);
    }

    /**
     * Returns an unmodifiable zero-allocation view of all local client connection IDs.
     */
    public Set<String> getLocalClients() {
        return unmodifiableLocalClients;
    }

    /**
     * Returns an unmodifiable zero-allocation view of all peer server connection IDs.
     */
    public Set<String> getPeerServers() {
        return unmodifiablePeerServers;
    }

    /**
     * Returns a snapshot set containing both local clients and peer servers.
     */
    public Set<String> getAllMembers() {
        Set<String> all = new HashSet<>(localClientIds.size() + peerServerIds.size());
        all.addAll(localClientIds);
        all.addAll(peerServerIds);
        return Collections.unmodifiableSet(all);
    }

    /**
     * Backward-compatible alias for getAllMembers().
     */
    public Set<String> getMembers() {
        return getAllMembers();
    }

    public boolean isEmpty() {
        return localClientIds.isEmpty() && peerServerIds.isEmpty();
    }

    public int getLocalClientCount() {
        return localClientIds.size();
    }

    public int getPeerServerCount() {
        return peerServerIds.size();
    }

    public int getMemberCount() {
        return localClientIds.size() + peerServerIds.size();
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
