package com.anilyanai.connector.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * =========================================================================================
 * ⚙️ ConnectorProperties (High-Performance Transport & Threading Configuration)
 * =========================================================================================
 *
 * <p>Central configuration properties for TCP and UDP network transports, dedicated NIO
 * selector loops, and isolated FIFO worker execution pools.</p>
 */
@ConfigurationProperties(prefix = "collab.connector")
@Getter
@Setter
public class ConnectorProperties {

    // =========================================================================
    // 1. TCP Inbound Transport & Worker Pool
    // =========================================================================

    /**
     * Whether TCP transport is enabled (default: true).
     */
    private boolean tcpEnabled = true;

    /**
     * Port for client TCP connections (default: 8080).
     */
    private int tcpPort = 8080;

    /**
     * Number of dedicated platform worker threads for executing inbound TCP frames (default: 16).
     */
    private int tcpInboundWorkerThreads = 16;

    /**
     * Batch size for grouping ready TCP frames across client channels before worker submission (default: 64).
     */
    private int tcpInboundBatchSize = 64;

    /**
     * TCP Socket Send Buffer (SO_SNDBUF) in bytes (default: 128 KB).
     */
    private int tcpSendBufferSize = 128 * 1024;

    /**
     * TCP Socket Receive Buffer (SO_RCVBUF) in bytes (default: 128 KB).
     */
    private int tcpReceiveBufferSize = 128 * 1024;


    // =========================================================================
    // 2. TCP Outbound Write Selectors
    // =========================================================================

    /**
     * Number of dedicated NIO selector threads for TCP outbound write draining (default: 1).
     * Set to 2, 4, 8, etc. to scale outbound broadcast throughput across CPU cores.
     */
    private int tcpOutboundSelectorThreads = 1;


    // =========================================================================
    // 3. UDP Inbound Transport & Dedicated FIFO Worker Pool
    // =========================================================================

    /**
     * Whether UDP transport is enabled (default: true).
     */
    private boolean udpEnabled = true;

    /**
     * Port for UDP datagram traffic (default: 8080).
     */
    private int udpPort = 8080;

    /**
     * Number of worker threads in the dedicated UDP FIFO pool (default: 4).
     * Set to 1 for strict sequential FIFO execution, or > 1 for parallel multi-core processing.
     */
    private int udpInboundWorkerThreads = 4;

    /**
     * Batch size for grouping received UDP datagrams before submitting to the worker pool (default: 64).
     */
    private int udpInboundBatchSize = 64;


    // =========================================================================
    // 4. UDP Outbound Egress Sockets Pool
    // =========================================================================

    /**
     * Number of parallel non-blocking UDP egress channels in the outbound pool (default: 4).
     * Bypasses OS socket lock contention and uses off-heap direct buffers for broadcasts.
     */
    private int udpOutboundEgressChannels = 4;


    // =========================================================================
    // Backward Compatibility Aliases & Fallbacks
    // =========================================================================

    public int getInboundWorkerThreads() {
        return tcpInboundWorkerThreads;
    }

    public void setInboundWorkerThreads(int threads) {
        this.tcpInboundWorkerThreads = threads;
    }

    public int getOutboundSelectors() {
        return tcpOutboundSelectorThreads;
    }

    public void setOutboundSelectors(int selectors) {
        this.tcpOutboundSelectorThreads = selectors;
    }

    public int getUdpWorkerThreads() {
        return udpInboundWorkerThreads;
    }

    public void setUdpWorkerThreads(int threads) {
        this.udpInboundWorkerThreads = threads;
    }

    public int getUdpEgressChannels() {
        return udpOutboundEgressChannels;
    }

    public void setUdpEgressChannels(int channels) {
        this.udpOutboundEgressChannels = channels;
    }
}
