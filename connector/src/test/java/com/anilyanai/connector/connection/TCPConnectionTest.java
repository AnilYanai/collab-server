package com.anilyanai.connector.connection;

import com.anilyanai.connector.selector.OutboundTrafficSelector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TCPConnectionTest {

    private SocketChannel mockChannel;
    private OutboundTrafficSelector.OutboundReactor mockReactor;
    private TCPConnection tcpConnection;

    @BeforeEach
    void setUp() {
        mockChannel = mock(SocketChannel.class);
        mockReactor = mock(OutboundTrafficSelector.OutboundReactor.class);

        tcpConnection = new TCPConnection("conn-test-1", mockChannel);
        tcpConnection.setAssignedReactor(mockReactor);
    }

    @Test
    void testMemoryQueueingWithoutWorkerSyscalls() throws IOException {
        byte[] payload = new byte[]{1, 2, 3, 4, 5};
        tcpConnection.sendOutboundPayload(payload);

        // Worker thread does ZERO socket channel write syscalls!
        verify(mockChannel, never()).write(any(ByteBuffer.class));

        // Payload is safely in the outbound queue waiting for gathering batch write
        assertTrue(tcpConnection.hasPendingData());
        assertFalse(tcpConnection.isWriteOver());

        // Delegates to assigned outbound selector to manage write interest
        verify(mockReactor, times(1)).enableWrite(tcpConnection);
    }

    @Test
    void testSafeWritePerformsGatheringBatchWrite() throws IOException {
        when(mockChannel.write(any(ByteBuffer[].class), eq(0), anyInt())).thenAnswer((Answer<Long>) invocation -> {
            ByteBuffer[] buffers = invocation.getArgument(0);
            int count = invocation.getArgument(2);
            long total = 0;
            for (int i = 0; i < count; i++) {
                int rem = buffers[i].remaining();
                buffers[i].position(buffers[i].limit());
                total += rem;
            }
            return total;
        });

        // Queue 10 messages
        for (int i = 0; i < 10; i++) {
            tcpConnection.sendOutboundPayload(new byte[]{(byte) i});
        }
        assertEquals(10, tcpConnection.getOutboundQueue().size());

        // Background reactor drains queue via safeWrite()
        tcpConnection.safeWrite();

        // Must invoke gathering write channel.write(ByteBuffer[], 0, count) in ONE syscall!
        verify(mockChannel, times(1)).write(any(ByteBuffer[].class), eq(0), eq(10));
        assertTrue(tcpConnection.isWriteOver());
    }
}
