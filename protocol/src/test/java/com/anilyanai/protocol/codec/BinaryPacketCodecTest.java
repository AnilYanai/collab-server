package com.anilyanai.protocol.codec;

import com.anilyanai.protocol.Opcode;
import com.anilyanai.protocol.Packet;
import com.anilyanai.protocol.exception.MalformedPacketException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class BinaryPacketCodecTest {

    private BinaryPacketCodec codec;

    @BeforeEach
    void setUp() {
        codec = new BinaryPacketCodec();
    }

    @Test
    void testEncodeAndDecodeBroadcast() {
        byte[] payload = "{\"tool\":\"pen\",\"x\":120,\"y\":450}".getBytes(StandardCharsets.UTF_8);
        Packet original = Packet.ofBroadcast("room-whiteboard-101", payload);

        byte[] encoded = codec.encode(original);
        assertNotNull(encoded);
        assertTrue(encoded.length > payload.length);

        Packet decoded = codec.decode(encoded);
        assertEquals(original, decoded);
        assertEquals(Opcode.BROADCAST, decoded.opcode());
        assertEquals("room-whiteboard-101", decoded.sessionId());
        assertNull(decoded.targetId());
        assertArrayEquals(payload, decoded.payload());
    }

    @Test
    void testEncodeAndDecodeDirect() {
        byte[] payload = new byte[]{1, 2, 3, 4, 5};
        Packet original = Packet.ofDirect("game-lobby-7", "client-uuid-99", payload);

        byte[] encoded = codec.encode(original);
        Packet decoded = codec.decode(encoded);

        assertEquals(original, decoded);
        assertEquals(Opcode.DIRECT, decoded.opcode());
        assertTrue(decoded.isDirect());
        assertEquals("game-lobby-7", decoded.sessionId());
        assertEquals("client-uuid-99", decoded.targetId());
        assertArrayEquals(payload, decoded.payload());
    }

    @Test
    void testEncodeAndDecodeChunkedLargePacket() {
        byte[] chunkData = new byte[1024];
        for (int i = 0; i < chunkData.length; i++) {
            chunkData[i] = (byte) (i % 128);
        }

        Packet chunk = Packet.ofChunk(
                Opcode.BROADCAST,
                "video-session-42",
                null,
                9999,
                (short) 2,
                (short) 10,
                chunkData
        );

        byte[] encoded = codec.encode(chunk);
        Packet decoded = codec.decode(encoded);

        assertEquals(chunk, decoded);
        assertTrue(decoded.isChunked());
        assertEquals(9999, decoded.messageId());
        assertEquals(2, decoded.chunkIndex());
        assertEquals(10, decoded.totalChunks());
        assertArrayEquals(chunkData, decoded.payload());
    }

    @Test
    void testEmptyPayloadPacket() {
        Packet join = Packet.ofJoin("editor-session-alpha");
        byte[] encoded = codec.encode(join);
        Packet decoded = codec.decode(encoded);

        assertEquals(join, decoded);
        assertEquals(Opcode.JOIN_SESSION, decoded.opcode());
        assertEquals(0, decoded.payload().length);
    }

    @Test
    void testInvalidMagicByteThrowsException() {
        byte[] payload = "test".getBytes(StandardCharsets.UTF_8);
        Packet packet = Packet.ofBroadcast("s1", payload);
        byte[] encoded = codec.encode(packet);

        // Corrupt magic byte
        encoded[0] = 0x00;

        assertThrows(MalformedPacketException.class, () -> codec.decode(encoded));
    }

    @Test
    void testTruncatedDataThrowsException() {
        byte[] shortData = new byte[]{Packet.MAGIC_BYTE, 1, 1};
        assertThrows(MalformedPacketException.class, () -> codec.decode(shortData));
    }
}
