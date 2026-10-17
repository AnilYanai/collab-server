package com.anilyanai.business.peer;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * =========================================================================================
 * 🌐 PeerRoutingService (Cluster Mesh Topology & Remote Client Router)
 * =========================================================================================
 *
 * <p>Handles peer clustering topologies, tracking which remote participants are connected
 * behind which cluster peer nodes, and managing dynamic peer connectivity.</p>
 *
 * @author anilyanai
 */
@Slf4j
public class PeerRoutingService {

    private final PeerSupplier peerSupplier;
    private final PeerConnector peerConnector;

    /**
     * Maps peerConnectionId -> Set of remote clientIds connected to that peer.
     */
    private final Map<String, Set<String>> peerToRemoteClients = new ConcurrentHashMap<>();

    /**
     * Reverse lookup: remoteClientId -> peerConnectionId
     */
    private final Map<String, String> remoteClientToPeer = new ConcurrentHashMap<>();

    public PeerRoutingService(PeerSupplier peerSupplier, PeerConnector peerConnector) {
        this.peerSupplier = peerSupplier != null ? peerSupplier : Collections::emptySet;
        this.peerConnector = peerConnector != null ? peerConnector : host -> null;
    }

    /**
     * Dials a dynamic peer server host if not already connected.
     */
    public String ensurePeerConnected(String peerHost) {
        if (peerHost == null || peerHost.isBlank()) return null;
        return peerConnector.connectToPeer(peerHost);
    }

    /**
     * Returns all currently active cluster peer connection IDs.
     */
    public Set<String> getActiveClusterPeers() {
        return peerSupplier.getActivePeerConnectionIds();
    }

    /**
     * Associates a remote client with the peer node through which it connects.
     */
    public void registerRemoteClient(String peerConnectionId, String remoteClientId) {
        if (peerConnectionId == null || remoteClientId == null) return;
        peerToRemoteClients.computeIfAbsent(peerConnectionId, id -> ConcurrentHashMap.newKeySet()).add(remoteClientId);
        remoteClientToPeer.put(remoteClientId, peerConnectionId);
    }

    /**
     * Finds the peer connection ID hosting a remote client.
     */
    public String getPeerForRemoteClient(String remoteClientId) {
        return remoteClientId != null ? remoteClientToPeer.get(remoteClientId) : null;
    }

    /**
     * Evicts all remote client mappings for a disconnected peer node.
     */
    public void unregisterPeer(String peerConnectionId) {
        if (peerConnectionId == null) return;
        Set<String> clients = peerToRemoteClients.remove(peerConnectionId);
        if (clients != null) {
            for (String clientId : clients) {
                remoteClientToPeer.remove(clientId);
            }
        }
        log.debug("Unregistered remote clients for peer {}", peerConnectionId);
    }
}
