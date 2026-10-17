package com.anilyanai.connector;

import com.anilyanai.connector.configuration.ClusterProperties;
import com.anilyanai.connector.configuration.ConnectorProperties;
import com.anilyanai.connector.events.InboundMessageHandler;
import com.anilyanai.connector.events.OutboundMessageHandler;
import com.anilyanai.connector.registry.ConnectionRegistry;
import com.anilyanai.connector.selector.InboundPeerTrafficSelector;
import com.anilyanai.connector.selector.OutboundTrafficSelector;
import com.anilyanai.connector.selector.TCPInboundTrafficSelector;
import com.anilyanai.connector.selector.UDPInboundTrafficSelector;
import com.anilyanai.connector.transport.TCPTransport;
import com.anilyanai.connector.transport.UDPTransport;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Set;

@Slf4j
public class ConnectionManager {
    private final TCPTransport tcpTransport;
    private final UDPTransport udpTransport;
    private final TCPInboundTrafficSelector tcpInboundSelector;
    private final UDPInboundTrafficSelector udpInboundSelector;
    private final OutboundTrafficSelector outboundSelector;
    private final ConnectionRegistry connectionRegistry;
    private final ConnectorProperties properties;
    private final InboundPeerTrafficSelector peerSelector;
    private final ClusterProperties clusterProperties;

    /**
     * Modern full constructor with isolated TCP and UDP inbound reactors.
     */
    public ConnectionManager(TCPTransport tcpTransport,
                             UDPTransport udpTransport,
                             TCPInboundTrafficSelector tcpInboundSelector,
                             UDPInboundTrafficSelector udpInboundSelector,
                             OutboundTrafficSelector outboundSelector,
                             ConnectionRegistry connectionRegistry,
                             ConnectorProperties properties,
                             InboundPeerTrafficSelector peerSelector,
                             ClusterProperties clusterProperties) {
        this.tcpTransport = tcpTransport;
        this.udpTransport = udpTransport;
        this.tcpInboundSelector = tcpInboundSelector;
        this.udpInboundSelector = udpInboundSelector;
        this.outboundSelector = outboundSelector;
        this.connectionRegistry = connectionRegistry;
        this.properties = properties;
        this.peerSelector = peerSelector;
        this.clusterProperties = clusterProperties;
    }

    /**
     * Modern constructor without clustering.
     */
    public ConnectionManager(TCPTransport tcpTransport,
                             UDPTransport udpTransport,
                             TCPInboundTrafficSelector tcpInboundSelector,
                             UDPInboundTrafficSelector udpInboundSelector,
                             OutboundTrafficSelector outboundSelector,
                             ConnectionRegistry connectionRegistry,
                             ConnectorProperties properties) {
        this(tcpTransport, udpTransport, tcpInboundSelector, udpInboundSelector, outboundSelector, connectionRegistry, properties, null, null);
    }

    public void start(
            InboundMessageHandler incomingMessageHandler,
            OutboundMessageHandler outgoingMessageHandler)
            throws Exception {
        start(incomingMessageHandler, incomingMessageHandler, outgoingMessageHandler);
    }

    public void start(
            InboundMessageHandler incomingMessageHandler,
            InboundMessageHandler peerMessageHandler,
            OutboundMessageHandler outgoingMessageHandler)
            throws Exception {

        if (!properties.isTcpEnabled() && !properties.isUdpEnabled()) {
            throw new IllegalStateException("At least one transport protocol (TCP or UDP) must be enabled in configuration!");
        }

        outboundSelector.initialize(outgoingMessageHandler);

        // 1. Conditionally configure and start TCP Transport with dedicated TCP Reactor
        if (properties.isTcpEnabled()) {
            tcpInboundSelector.setProperties(properties);
            tcpInboundSelector.setOutboundTrafficSelector(outboundSelector);
            tcpInboundSelector.initialize(incomingMessageHandler);
            tcpTransport.initialize();
            tcpTransport.bind(properties.getTcpPort());
            tcpInboundSelector.registerServerChannel(tcpTransport.getChannel());
            tcpInboundSelector.start();
            log.info("🚀 Dedicated TCP Inbound Transport started on port {}", properties.getTcpPort());
        } else {
            log.info("TCP Transport is disabled by configuration");
        }

        // 2. Conditionally configure and start UDP Transport with dedicated wire-speed UDP Reactor
        if (properties.isUdpEnabled()) {
            udpInboundSelector.setUdpEgressPool(outboundSelector.getUdpEgressPool());
            udpInboundSelector.initialize(incomingMessageHandler);
            udpTransport.initialize();
            udpTransport.bind(properties.getUdpPort());
            udpInboundSelector.registerChannel(udpTransport.getChannel());
            udpInboundSelector.start();
            log.info("🚀 Dedicated Wire-Speed UDP Inbound Transport started on port {}", properties.getUdpPort());
        } else {
            log.info("UDP Transport is disabled by configuration");
        }

        // 3. Conditionally configure and start Peer Cluster Transport
        if (clusterProperties != null && clusterProperties.isEnabled() && peerSelector != null) {
            peerSelector.initialize(peerMessageHandler);
            peerSelector.start();
            log.info("🌐 Dedicated Cluster Peer Transport started on port {}", clusterProperties.getPort());
        } else {
            log.info("Standalone Mode: Cluster Peer Transport is disabled");
        }

        try {
            outboundSelector.start();
        } catch (Exception e) {
            if (tcpInboundSelector != null) tcpInboundSelector.stop();
            if (udpInboundSelector != null) udpInboundSelector.stop();
            outboundSelector.stop();
            if (peerSelector != null) {
                peerSelector.stop();
            }
            throw e;
        }
    }

    public void stop() {
        if (tcpInboundSelector != null) {
            tcpInboundSelector.stop();
        }
        if (udpInboundSelector != null) {
            udpInboundSelector.stop();
        }
        if (outboundSelector != null) {
            outboundSelector.stop();
        }
        if (peerSelector != null) {
            peerSelector.stop();
        }
    }

    public void publishMessage(
            String id,
            Object message
    ) {
        outboundSelector.submitOutboundMessage(id, message);
    }

    /**
     * Dynamically establishes a peer connection to a remote cluster node on demand.
     *
     * @param peerAddress host:port of the remote peer node
     * @return the peer connection ID if connected/connecting, or null if clustering is disabled
     */
    public String connectToPeer(String peerAddress) {
        if (peerSelector != null && clusterProperties != null && clusterProperties.isEnabled()) {
            return peerSelector.connectToPeer(peerAddress);
        }
        return null;
    }

    public ConnectionRegistry getConnectionRegistry() {
        return connectionRegistry;
    }

    public ConnectorProperties getProperties() {
        return properties;
    }

    public ClusterProperties getClusterProperties() {
        return clusterProperties;
    }

    public Set<String> getActivePeerConnectionIds() {
        return (peerSelector != null) ? peerSelector.getActivePeerConnectionIds() : Collections.emptySet();
    }
}
