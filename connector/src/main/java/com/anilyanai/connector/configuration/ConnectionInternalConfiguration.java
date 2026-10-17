package com.anilyanai.connector.configuration;

import com.anilyanai.connector.executor.WorkerPool;
import com.anilyanai.connector.registry.ConnectionRegistry;
import com.anilyanai.connector.selector.InboundPeerTrafficSelector;
import com.anilyanai.connector.selector.OutboundTrafficSelector;
import com.anilyanai.connector.selector.TCPInboundTrafficSelector;
import com.anilyanai.connector.selector.UDPInboundTrafficSelector;
import com.anilyanai.connector.transport.TCPTransport;
import com.anilyanai.connector.transport.UDPEgressPool;
import com.anilyanai.connector.transport.UDPTransport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({ConnectorProperties.class, ClusterProperties.class})
@Slf4j
class ConnectionInternalConfiguration {

    public ConnectionInternalConfiguration() {
        log.info("Internal connector configuration loaded");
    }

    @Bean
    TCPTransport tcpTransport() {
        return new TCPTransport();
    }

    @Bean
    UDPTransport udpTransport() {
        return new UDPTransport();
    }

    @Bean
    UDPEgressPool udpEgressPool(ConnectorProperties properties) {
        return new UDPEgressPool(properties.getUdpPort(), properties.getUdpOutboundEgressChannels());
    }

    @Bean
    TCPInboundTrafficSelector tcpInboundSelector(
            ConnectionRegistry registry,
            @Qualifier("tcpWorkerPool")
            WorkerPool tcpWorkerPool,
            ConnectorProperties properties
    ) {
        return new TCPInboundTrafficSelector(
                registry,
                tcpWorkerPool,
                properties
        );
    }

    @Bean
    UDPInboundTrafficSelector udpInboundSelector(
            ConnectionRegistry registry,
            @Qualifier("udpWorkerPool")
            WorkerPool udpWorkerPool,
            ConnectorProperties properties
    ) {
        return new UDPInboundTrafficSelector(
                registry,
                udpWorkerPool,
                properties
        );
    }

    @Bean
    OutboundTrafficSelector outboundSelector(
            ConnectionRegistry registry,
            ConnectorProperties properties,
            UDPEgressPool udpEgressPool
    ) {
        return new OutboundTrafficSelector(
                registry,
                properties.getTcpOutboundSelectorThreads(),
                udpEgressPool
        );
    }

    @Bean
    @ConditionalOnProperty(prefix = "collab.cluster", name = "enabled", havingValue = "true")
    InboundPeerTrafficSelector inboundPeerTrafficSelector(
            ClusterProperties clusterProperties,
            ConnectionRegistry registry,
            @Qualifier("peerWorkerPool")
            WorkerPool peerWorkerPool
    ) {
        return new InboundPeerTrafficSelector(
                clusterProperties,
                registry,
                peerWorkerPool
        );
    }

    @Bean
    @ConditionalOnProperty(prefix = "collab.cluster", name = "enabled", havingValue = "true")
    WorkerPool peerWorkerPool(ClusterProperties clusterProperties) {
        return new WorkerPool(clusterProperties.getWorkerThreads(), "peer-worker");
    }

    @Bean
    ConnectionRegistry connectionRegistry() {
        return new ConnectionRegistry();
    }

    @Bean
    WorkerPool tcpWorkerPool(ConnectorProperties properties) {
        return new WorkerPool(properties.getTcpInboundWorkerThreads(), "tcp-worker");
    }

    @Bean
    WorkerPool udpWorkerPool(ConnectorProperties properties) {
        return new WorkerPool(properties.getUdpInboundWorkerThreads(), "udp-worker");
    }
}
