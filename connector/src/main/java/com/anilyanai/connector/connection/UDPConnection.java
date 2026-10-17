package com.anilyanai.connector.connection;

import com.anilyanai.connector.connection.model.ReadResult;
import com.anilyanai.connector.transport.UDPEgressPool;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

@Slf4j
public class UDPConnection extends Connection {
    private final DatagramChannel channel;
    private final InetSocketAddress address;

    /**
     * Pre-assigned egress datagram channel cached ONCE on connection registration.
     * Transmitting directly through this cached channel eliminates all hashing, modulo math,
     * and array lookups from the hot per-packet outbound path.
     */
    private DatagramChannel egressChannel;

    public UDPConnection(
            String connectionId,
            DatagramChannel channel,
            InetSocketAddress address
    ) {
        super(connectionId);
        this.channel = channel;
        this.address = address;
        this.egressChannel = channel; // Default to inbound channel until egress channel is assigned
    }

    public void setEgressChannel(DatagramChannel egressChannel) {
        if (egressChannel != null) {
            this.egressChannel = egressChannel;
        }
    }

    public DatagramChannel getEgressChannel() {
        return egressChannel;
    }

    /**
     * Polymorphic branchless direct UDP transmission.
     * Uses pre-cached egress channel and off-heap direct buffer without any instanceof checks.
     */
    @Override
    public void sendOutboundPayload(byte[] payload) throws IOException {
        touchActive();
        UDPEgressPool.transmitPayload(egressChannel, payload, address);
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException {
        touchActive();
        egressChannel.send(buffer, address);
    }

    @Override
    public ReadResult read() throws IOException {
        touchActive();
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        SocketAddress source = channel.receive(buffer);
        if (source == null) {
            return null;
        }

        buffer.flip();

        return new ReadResult(
                buffer,
                source
        );
    }

    public SocketAddress getAddress() {
        return address;
    }

    @Override
    public DatagramChannel getChannel() {
        return channel;
    }

    @Override
    public void close() {
        // UDP has no TCP-style connection teardown
    }

    @Override
    public void registerInto(SessionRegistrationTarget target) {
        if (target != null) {
            target.registerUDP(this);
        }
    }

    @Override
    public void unregisterFrom(SessionRegistrationTarget target) {
        if (target != null) {
            target.unregisterUDP(getConnectionId());
        }
    }
}
