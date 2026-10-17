package com.anilyanai.business.configuration;

import com.anilyanai.business.broadcast.BroadcastService;
import com.anilyanai.business.broadcast.MessagePublisher;
import com.anilyanai.business.dispatcher.ClientInboundRouter;
import com.anilyanai.business.dispatcher.PacketDispatcher;
import com.anilyanai.business.dispatcher.PeerInboundRouter;
import com.anilyanai.business.peer.PeerRoutingService;
import com.anilyanai.business.security.AdminAuthorizer;
import com.anilyanai.business.security.HmacAdminAuthorizer;
import com.anilyanai.business.security.HmacTokenAuthorizer;
import com.anilyanai.business.security.SessionAuthorizer;
import com.anilyanai.business.state.SessionStateManager;
import com.anilyanai.connector.ConnectionManager;
import com.anilyanai.connector.registry.ConnectionRegistry;
import com.anilyanai.engine.session.SessionRegistry;
import com.anilyanai.protocol.codec.BinaryPacketCodec;
import com.anilyanai.protocol.codec.PacketCodec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
public class CollabBusinessAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SessionStateManager sessionStateManager(SessionRegistry sessionRegistry) {
        SessionStateManager stateManager = new SessionStateManager();
        // Wire session closure state cleanup via method reference (::)
        sessionRegistry.registerSessionCloseListener(stateManager::clearSession);
        return stateManager;
    }

    @Bean
    @ConditionalOnMissingBean
    public PeerRoutingService peerRoutingService(ConnectionManager connectionManager) {
        if (connectionManager != null) {
            return new PeerRoutingService(
                    connectionManager::getActivePeerConnectionIds,
                    connectionManager::connectToPeer
            );
        }
        return new PeerRoutingService(null, null);
    }

    @Bean
    @ConditionalOnMissingBean
    public BroadcastService broadcastService(
            SessionRegistry sessionRegistry,
            ConnectionManager connectionManager) {
        MessagePublisher publisher = (connectionManager != null)
                ? connectionManager::publishMessage
                : (connId, bytes) -> {};
        return new BroadcastService(
                sessionRegistry,
                publisher
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public PacketCodec packetCodec() {
        return new BinaryPacketCodec();
    }

    @Bean
    @ConditionalOnProperty(prefix = "collab.security", name = "hmac-secret")
    public SessionAuthorizer hmacSessionAuthorizer(
            @Value("${collab.security.hmac-secret}") String hmacSecret) {
        return new HmacTokenAuthorizer(hmacSecret);
    }

    @Bean
    @ConditionalOnMissingBean(SessionAuthorizer.class)
    public SessionAuthorizer permissiveSessionAuthorizer() {
        return SessionAuthorizer.PERMISSIVE;
    }

    @Bean
    @ConditionalOnProperty(prefix = "collab.security", name = "admin-secret")
    public AdminAuthorizer hmacAdminAuthorizer(
            @Value("${collab.security.admin-secret}") String adminSecret) {
        return new HmacAdminAuthorizer(adminSecret);
    }

    @Bean
    @ConditionalOnMissingBean(AdminAuthorizer.class)
    public AdminAuthorizer permissiveAdminAuthorizer() {
        return AdminAuthorizer.PERMISSIVE;
    }

    @Bean
    @ConditionalOnMissingBean
    public ClientInboundRouter clientInboundRouter(
            SessionRegistry sessionRegistry,
            ConnectionManager connectionManager,
            PacketCodec packetCodec,
            SessionAuthorizer sessionAuthorizer,
            AdminAuthorizer adminAuthorizer,
            BroadcastService broadcastService,
            PeerRoutingService peerRoutingService) {

        ConnectionRegistry connectionRegistry = (connectionManager != null)
                ? connectionManager.getConnectionRegistry()
                : null;

        MessagePublisher publisher = (connectionManager != null)
                ? connectionManager::publishMessage
                : (connId, bytes) -> {};

        return new ClientInboundRouter(
                sessionRegistry,
                publisher,
                packetCodec,
                sessionAuthorizer,
                adminAuthorizer,
                broadcastService,
                peerRoutingService,
                connectionRegistry
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public PeerInboundRouter peerInboundRouter(
            SessionRegistry sessionRegistry,
            ConnectionManager connectionManager,
            PacketCodec packetCodec,
            BroadcastService broadcastService) {

        MessagePublisher publisher = (connectionManager != null)
                ? connectionManager::publishMessage
                : (connId, bytes) -> {};

        return new PeerInboundRouter(
                sessionRegistry,
                publisher,
                packetCodec,
                broadcastService
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public PacketDispatcher packetDispatcher(
            SessionRegistry sessionRegistry,
            ClientInboundRouter clientInboundRouter,
            PeerInboundRouter peerInboundRouter,
            ConnectionManager connectionManager,
            PacketCodec packetCodec,
            SessionStateManager sessionStateManager,
            PeerRoutingService peerRoutingService) {

        ConnectionRegistry connectionRegistry = (connectionManager != null)
                ? connectionManager.getConnectionRegistry()
                : null;

        // Bind disconnect listener via method reference (::)
        if (connectionRegistry != null) {
            connectionRegistry.registerDisconnectListener(sessionRegistry::removeConnection);
            connectionRegistry.registerDisconnectListener(peerRoutingService::unregisterPeer);
        }

        return new PacketDispatcher(
                sessionRegistry,
                clientInboundRouter,
                peerInboundRouter,
                packetCodec,
                sessionStateManager
        );
    }
}
