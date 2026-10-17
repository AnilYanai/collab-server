package com.anilyanai.protocol.codec;

import com.anilyanai.protocol.Packet;
import com.anilyanai.protocol.exception.MalformedPacketException;

import java.nio.ByteBuffer;

/**
 * Strategy interface for serializing and deserializing protocol packets.
 */
public interface PacketCodec {

    /**
     * Serializes a high-level {@link Packet} into its raw wire format.
     *
     * @param packet the packet to encode
     * @return serialized raw byte array
     */
    byte[] encode(Packet packet);

    /**
     * Deserializes a raw byte array into a high-level {@link Packet}.
     *
     * @param rawData raw byte array
     * @return deserialized packet
     * @throws MalformedPacketException if the wire format is invalid or corrupt
     */
    Packet decode(byte[] rawData) throws MalformedPacketException;

    /**
     * Deserializes from a {@link ByteBuffer} into a high-level {@link Packet}.
     *
     * @param buffer source byte buffer
     * @return deserialized packet
     * @throws MalformedPacketException if the wire format is invalid or corrupt
     */
    Packet decode(ByteBuffer buffer) throws MalformedPacketException;
}
