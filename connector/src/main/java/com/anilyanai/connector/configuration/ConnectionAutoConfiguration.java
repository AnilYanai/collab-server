package com.anilyanai.connector.configuration;

import com.anilyanai.connector.ConnectionManager;
import com.anilyanai.connector.registry.ConnectionRegistry;
import com.anilyanai.connector.selector.InboundPeerTrafficSelector;
import com.anilyanai.connector.selector.OutboundTrafficSelector;
import com.anilyanai.connector.selector.TCPInboundTrafficSelector;
import com.anilyanai.connector.selector.UDPInboundTrafficSelector;
import com.anilyanai.connector.transport.TCPTransport;
import com.anilyanai.connector.transport.UDPTransport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@AutoConfiguration
@ConditionalOnMissingBean(ConnectionManager.class)
@Import(ConnectionInternalConfiguration.class)
@Slf4j
public class ConnectionAutoConfiguration {

    public ConnectionAutoConfiguration() {
        log.info("AUTO CONFIG LOADED");
    }

    @Bean
    public ConnectionManager connectionManager(
            TCPTransport tcpTransport,
            UDPTransport udpTransport,
            ConnectionRegistry registry,
            TCPInboundTrafficSelector tcpInboundSelector,
            UDPInboundTrafficSelector udpInboundSelector,
            OutboundTrafficSelector outboundSelector,
            ConnectorProperties properties,
            @Autowired(required = false) InboundPeerTrafficSelector peerSelector,
            @Autowired(required = false) ClusterProperties clusterProperties
    ) {
        return new ConnectionManager(
                tcpTransport,
                udpTransport,
                tcpInboundSelector,
                udpInboundSelector,
                outboundSelector,
                registry,
                properties,
                peerSelector,
                clusterProperties
        );
    }
}
