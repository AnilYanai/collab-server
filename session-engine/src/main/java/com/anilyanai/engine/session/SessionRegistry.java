package com.anilyanai.engine.session;

import com.anilyanai.connector.connection.Connection;
import com.anilyanai.connector.connection.TCPConnection;
import com.anilyanai.connector.connection.UDPConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * =========================================================================================
 * 🚀 SessionRegistry (Ultra-Fast Thread-Safe Room Management Registry)
 * =========================================================================================
 *
 * <p>Manages live collaboration sessions, client-to-session routing tables, and peer cluster
 * mesh tracking with lock-free concurrent structures.</p>
 *
 * <h3>Design & Performance Characteristics:</h3>
 * <ul>
 *   <li><b>Single Session Fetch:</b> Provides {@link #getSession(String)} so high-frequency
 *       packet fan-out pipelines execute exactly 1 map lookup instead of quadruple searches.</li>
 *   <li><b>Partitioned Routing Views:</b> Exposes {@link #getLocalClients(String)} and
 *       {@link #getPeerServers(String)} separately to eliminate routing condition checks.</li>
 *   <li><b>Direct Connection Fan-Out:</b> Exposes {@link #getUDPConnections(String)} and
 *       {@link #getTCPConnections(String)} for direct wire-speed broadcast without ConcurrentHashMap lookups.</li>
 *   <li><b>Polymorphic State Buffering:</b> Configures session state preservation via
 *       {@link StatePreserver#create(boolean)} on session creation, eliminating checks during fan-out.</li>
 *   <li><b>Decoupled Event Callbacks:</b> Supports registration of lifecycle listeners via method
 *       references ({@code ::}) for session close and member leave events.</li>
 * </ul>
 *
 * @author anilyanai
 */
@Slf4j
public class SessionRegistry {

    private final Map<String, Session> sessions = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> connectionToSessions = new ConcurrentHashMap<>();

    private final List<Consumer<String>> sessionCloseListeners = new CopyOnWriteArrayList<>();
    private final List<BiConsumer<String, String>> memberLeaveListeners = new CopyOnWriteArrayList<>();

    /**
     * Fast-path direct lookup returning the live Session container.
     * Allows callers to perform a single map lookup and reuse the session instance across fan-out.
     */
    public Session getSession(String sessionId) {
        if (sessionId == null) return null;
        return sessions.get(sessionId);
    }

    /**
     * Joins a connection to a session (ephemeral by default).
     */
    public void join(String sessionId, String connectionId) {
        join(sessionId, connectionId, false);
    }

    /**
     * Joins a Connection directly to a session with state preservation specification.
     * Caching direct Connection objects eliminates hash table lookups during fan-out.
     */
    public void join(String sessionId, Connection connection, boolean preserveState) {
        if (sessionId == null || connection == null) return;

        Session session = sessions.computeIfAbsent(sessionId, id -> {
            Session s = new Session(id);
            s.setStatePreserver(StatePreserver.create(preserveState));
            return s;
        });

        session.joinLocalClient(connection);
        connectionToSessions.computeIfAbsent(connection.getConnectionId(), k -> ConcurrentHashMap.newKeySet()).add(sessionId);
    }

    /**
     * Joins a connection to a session with state preservation specification (String ID fallback).
     */
    public void join(String sessionId, String connectionId, boolean preserveState) {
        if (sessionId == null || connectionId == null) return;

        Session session = sessions.computeIfAbsent(sessionId, id -> {
            Session s = new Session(id);
            s.setStatePreserver(StatePreserver.create(preserveState));
            return s;
        });

        session.joinLocalClient(connectionId);
        connectionToSessions.computeIfAbsent(connectionId, k -> ConcurrentHashMap.newKeySet()).add(sessionId);
    }

    /**
     * Joins a remote cluster peer server to a session.
     */
    public void joinPeer(String sessionId, String peerConnectionId) {
        if (sessionId == null || peerConnectionId == null) return;

        Session session = sessions.computeIfAbsent(sessionId, id -> {
            Session s = new Session(id);
            s.setStatePreserver(StatePreserver.create(false));
            return s;
        });

        session.joinPeerServer(peerConnectionId);
        connectionToSessions.computeIfAbsent(peerConnectionId, k -> ConcurrentHashMap.newKeySet()).add(sessionId);
    }

    /**
     * Saves raw packet state into the session preserver without condition checks.
     */
    public void saveSessionState(String sessionId, byte[] rawBytes) {
        Session session = getSession(sessionId);
        if (session != null) {
            session.saveState(rawBytes);
        }
    }

    /**
     * Retrieves preserved session history.
     */
    public List<byte[]> getSessionHistory(String sessionId) {
        Session session = getSession(sessionId);
        return session != null ? session.getHistory() : Collections.emptyList();
    }

    /**
     * Removes a connection from a specific session.
     */
    public void leave(String sessionId, String connectionId) {
        if (sessionId == null || connectionId == null) return;

        Session session = sessions.get(sessionId);
        if (session != null) {
            session.leave(connectionId);
            notifyMemberLeave(sessionId, connectionId);

            if (session.isEmpty()) {
                closeSession(sessionId);
            }
        }

        Set<String> connSessions = connectionToSessions.get(connectionId);
        if (connSessions != null) {
            connSessions.remove(sessionId);
            if (connSessions.isEmpty()) {
                connectionToSessions.remove(connectionId);
            }
        }
    }

    /**
     * Completely evicts a connection from all active sessions it was a part of.
     */
    public void removeConnection(String connectionId) {
        if (connectionId == null) return;

        Set<String> joinedSessions = connectionToSessions.remove(connectionId);
        if (joinedSessions == null || joinedSessions.isEmpty()) return;

        for (String sessionId : joinedSessions) {
            Session session = sessions.get(sessionId);
            if (session != null) {
                session.leave(connectionId);
                notifyMemberLeave(sessionId, connectionId);
                if (session.isEmpty()) {
                    closeSession(sessionId);
                }
            }
        }
    }

    /**
     * Closes and dismantles an entire session, evicting all members and notifying listeners.
     */
    public Set<String> closeSession(String sessionId) {
        if (sessionId == null) return Collections.emptySet();

        Session session = sessions.remove(sessionId);
        if (session == null) return Collections.emptySet();

        Set<String> members = session.getAllMembers();
        for (String memberId : members) {
            Set<String> connSessions = connectionToSessions.get(memberId);
            if (connSessions != null) {
                connSessions.remove(sessionId);
                if (connSessions.isEmpty()) {
                    connectionToSessions.remove(memberId);
                }
            }
        }

        session.clearState();
        notifySessionClose(sessionId);

        log.info("Session {} closed. Evicted {} members.", sessionId, members.size());
        return members;
    }

    /**
     * Returns direct typed UDP connections for wire-speed broadcast with zero instanceof checks.
     */
    public Collection<UDPConnection> getUDPConnections(String sessionId) {
        Session session = sessions.get(sessionId);
        return (session != null) ? session.getUDPConnections() : Collections.emptyList();
    }

    /**
     * Returns direct typed TCP connections for reliable broadcast with zero instanceof checks.
     */
    public Collection<TCPConnection> getTCPConnections(String sessionId) {
        Session session = sessions.get(sessionId);
        return (session != null) ? session.getTCPConnections() : Collections.emptyList();
    }

    /**
     * Returns direct Connection references for backward-compatible access.
     */
    public Collection<Connection> getLocalConnections(String sessionId) {
        Session session = sessions.get(sessionId);
        return (session != null) ? session.getLocalConnections() : Collections.emptyList();
    }

    /**
     * Fast-path lookup for local client member IDs.
     */
    public Set<String> getLocalClients(String sessionId) {
        Session session = sessions.get(sessionId);
        return (session != null) ? session.getLocalClients() : Collections.emptySet();
    }

    /**
     * Fast-path lookup for peer cluster server members only.
     */
    public Set<String> getPeerServers(String sessionId) {
        Session session = sessions.get(sessionId);
        return (session != null) ? session.getPeerServers() : Collections.emptySet();
    }

    /**
     * Returns all members (local clients and peers).
     */
    public Set<String> getSessionMembers(String sessionId) {
        Session session = sessions.get(sessionId);
        return (session != null) ? session.getAllMembers() : Collections.emptySet();
    }

    /**
     * Alias for getSessionMembers(sessionId).
     */
    public Set<String> getMembers(String sessionId) {
        return getSessionMembers(sessionId);
    }

    /**
     * Returns all sessions a given connection currently belongs to.
     */
    public Set<String> getSessions(String connectionId) {
        if (connectionId == null) return Collections.emptySet();
        Set<String> set = connectionToSessions.get(connectionId);
        return (set != null) ? Collections.unmodifiableSet(set) : Collections.emptySet();
    }

    public boolean sessionExists(String sessionId) {
        return sessionId != null && sessions.containsKey(sessionId);
    }

    public int getSessionCount() {
        return sessions.size();
    }

    public int getMemberCount(String sessionId) {
        Session session = sessions.get(sessionId);
        return (session != null) ? session.getMemberCount() : 0;
    }

    // =========================================================================
    // Lifecycle Listener Registration (Hooked via Method References ::)
    // =========================================================================

    public void registerSessionCloseListener(Consumer<String> listener) {
        if (listener != null) {
            sessionCloseListeners.add(listener);
        }
    }

    public void registerMemberLeaveListener(BiConsumer<String, String> listener) {
        if (listener != null) {
            memberLeaveListeners.add(listener);
        }
    }

    private void notifySessionClose(String sessionId) {
        for (Consumer<String> listener : sessionCloseListeners) {
            try {
                listener.accept(sessionId);
            } catch (Exception e) {
                log.warn("Error in session close listener: {}", e.getMessage());
            }
        }
    }

    private void notifyMemberLeave(String sessionId, String memberId) {
        for (BiConsumer<String, String> listener : memberLeaveListeners) {
            try {
                listener.accept(sessionId, memberId);
            } catch (Exception e) {
                log.warn("Error in member leave listener: {}", e.getMessage());
            }
        }
    }
}
