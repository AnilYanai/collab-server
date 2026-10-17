package com.anilyanai.connector.registry;

import com.anilyanai.connector.connection.Connection;
import com.anilyanai.connector.connection.UDPConnection;
import com.anilyanai.connector.events.DisconnectListener;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class ConnectionRegistry {

    // Primary lookup by connectionId (used for routing outbound messages)
    private final Map<String, Connection> connections =
            new ConcurrentHashMap<>();

    // Direct O(1) UDP address-to-connection lookup for inbound packet processing
    private final Map<InetSocketAddress, UDPConnection> udpAddressIndex =
            new ConcurrentHashMap<>();

    // Listeners notified when a connection disconnects
    private final List<DisconnectListener> disconnectListeners =
            new CopyOnWriteArrayList<>();

    public void registerDisconnectListener(DisconnectListener listener) {
        if (listener != null) {
            disconnectListeners.add(listener);
        }
    }

    public void add(Connection connection) {
        log.debug("Adding connection {} to registry", connection);

        connections.put(
                connection.getConnectionId(),
                connection
        );

        if (connection instanceof UDPConnection udpConnection) {
            udpAddressIndex.put(
                    (InetSocketAddress) udpConnection.getAddress(),
                    udpConnection
            );
        }
    }

    public Connection getConnection(String id) {
        return connections.get(id);
    }

    // Direct single-lookup O(1) without intermediate String ID or casting
    public UDPConnection getUDPConnection(InetSocketAddress address) {
        return udpAddressIndex.get(address);
    }

    public void removeConnection(String id) {
        Connection connection = connections.remove(id);

        if (connection instanceof UDPConnection udpConnection) {
            udpAddressIndex.remove(
                    (InetSocketAddress) udpConnection.getAddress()
            );
        }

        if (connection != null) {
            // Notify all disconnect listeners for immediate cleanup (e.g. SessionRegistry)
            for (DisconnectListener listener : disconnectListeners) {
                try {
                    listener.onDisconnect(id);
                } catch (Exception e) {
                    log.error("Error invoking disconnect listener for connection {}", id, e);
                }
            }
        }
    }

    public Collection<Connection> getAllConnection() {
        return connections.values();
    }
}
