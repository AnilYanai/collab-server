package com.anilyanai.application.runner;

import com.anilyanai.business.dispatcher.PacketDispatcher;
import com.anilyanai.connector.ConnectionManager;
import com.anilyanai.engine.session.SessionRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Starts the Collab Server network listeners and wires the PacketDispatcher
 * using method references (::) for decoupled message processing across multi-room sessions.
 */
@Component
@Slf4j
public class CollabServerRunner implements CommandLineRunner {

    private final ConnectionManager connectionManager;
    private final PacketDispatcher packetDispatcher;
    private final SessionRegistry sessionRegistry;
    private final ScheduledExecutorService monitorExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "session-monitor");
        t.setDaemon(true);
        return t;
    });

    public CollabServerRunner(ConnectionManager connectionManager,
                              PacketDispatcher packetDispatcher,
                              SessionRegistry sessionRegistry) {
        this.connectionManager = connectionManager;
        this.packetDispatcher = packetDispatcher;
        this.sessionRegistry = sessionRegistry;
    }

    @Override
    public void run(String... args) {
        try {
            log.info("🚀 Bootstrapping Collab Server with Multi-Session Packet Routing...");
            
            // Wire PacketDispatcher via decoupled method references (::)
            // Passes onReceive for normal clients and onPeerReceive for cluster peers
            connectionManager.start(
                    packetDispatcher::onReceive,
                    packetDispatcher::onPeerReceive,
                    packetDispatcher::onSend
            );
            
            log.info("✅ Collab Server is live and routing packets across TCP/UDP!");

            // Periodic session monitor logging for visibility during multi-session test & benchmark
            monitorExecutor.scheduleAtFixedRate(() -> {
                int activeSessions = sessionRegistry.getSessionCount();
                if (activeSessions > 0) {
                    log.info("📊 Active Rooms/Sessions: {}", activeSessions);
                }
            }, 5, 5, TimeUnit.SECONDS);

        } catch (Exception e) {
            log.error("Failed to start Collab Server: {}", e.getMessage(), e);
        }
    }
}
