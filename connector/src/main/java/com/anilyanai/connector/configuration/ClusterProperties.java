package com.anilyanai.connector.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * =========================================================================================
 * 🌐 ClusterProperties (Multi-Node Server Mesh Configuration)
 * =========================================================================================
 *
 * <p>Controls the dedicated inter-server cluster transport.
 * Allows the collaboration server to run either in ultra-fast standalone mode (default)
 * or as a clustered mesh of communicating broadcast nodes.</p>
 *
 * <ul>
 *   <li><b>Standalone Mode:</b> {@code enabled = false} (default). Zero cluster ports opened,
 *       zero peer threads allocated. Runs at maximum single-node speed.</li>
 *   <li><b>Clustered Mode:</b> {@code enabled = true}. Spins up {@code InboundPeerTrafficSelector}
 *       on {@code port} (default 9090) and connects out to configured {@code peers}.</li>
 * </ul>
 *
 * @author anilyanai
 */
@ConfigurationProperties(prefix = "collab.cluster")
@Getter
@Setter
public class ClusterProperties {

    /**
     * Whether clustering / inter-server mesh scaling is enabled.
     * Default is false (standalone mode).
     */
    private boolean enabled = false;

    /**
     * Dedicated TCP port for private inter-server peer communication.
     * Keeps high-speed cluster traffic isolated from end-user traffic.
     */
    private int port = 9090;

    /**
     * List of remote peer server addresses (e.g. "127.0.0.1:9091", "10.0.0.5:9090").
     * The server connects outbound to these nodes on startup to form the mesh.
     */
    private List<String> peers = new ArrayList<>();

    /**
     * Number of worker threads dedicated to handling incoming peer cluster packets.
     */
    private int workerThreads = 4;
}
