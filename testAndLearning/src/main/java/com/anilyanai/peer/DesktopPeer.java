package com.anilyanai.peer;

import com.anilyanai.business.security.HmacTokenAuthorizer;
import com.anilyanai.protocol.Opcode;
import com.anilyanai.protocol.Packet;
import com.anilyanai.protocol.codec.BinaryPacketCodec;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * =========================================================================================
 * 💻 Interactive Desktop Peer (Pairs with Android Mobile App)
 * =========================================================================================
 *
 * <p>Run this on your computer while running the Android app on your phone!</p>
 * <ul>
 *   <li>Joins session: <b>mobile-room-1</b></li>
 *   <li>User ID: <b>DesktopPC</b></li>
 *   <li>Type any message into this console to broadcast to your Android phone</li>
 *   <li>Displays incoming messages from your Android phone in real time with latency!</li>
 * </ul>
 */
public class DesktopPeer {

    private static final String HOST = "localhost";
    private static final int PORT = 8080;
    private static final String SESSION_ID = "mobile-room-1";
    private static final String USER_ID = "DesktopPC";
    private static final String SHARED_SECRET = "collab-benchmark-secret-key-256bit-ok";

    private static final BinaryPacketCodec CODEC = new BinaryPacketCodec();
    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) throws Exception {
        InetSocketAddress serverAddress = new InetSocketAddress(HOST, PORT);
        DatagramSocket socket = new DatagramSocket();

        System.out.println("=================================================================");
        System.out.println("💻 INTERACTIVE DESKTOP PEER (COLLAB-SERVER)");
        System.out.println("=================================================================");
        System.out.println("Target Server : " + HOST + ":" + PORT + " (UDP)");
        System.out.println("Session / Room: " + SESSION_ID);
        System.out.println("Your User ID  : " + USER_ID);
        System.out.println("=================================================================\n");

        // 1. Generate HMAC Token & Send JOIN
        String token = HmacTokenAuthorizer.generateToken(SESSION_ID, USER_ID, 3600, SHARED_SECRET);
        byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_8);

        Packet joinPacket = Packet.ofJoin(SESSION_ID);
        // Include token as payload
        Packet secureJoin = new Packet(
                Packet.CURRENT_VERSION,
                Opcode.JOIN_SESSION,
                Packet.FLAG_NONE,
                101,
                (short) 0,
                (short) 1,
                SESSION_ID,
                null,
                tokenBytes
        );

        byte[] joinBytes = CODEC.encode(secureJoin);
        socket.send(new DatagramPacket(joinBytes, joinBytes.length, serverAddress));
        System.out.println("⏳ Join request sent to server. Listening for server ACK and messages...\n");

        // 2. Background receiver thread
        Thread.ofVirtual().name("desktop-peer-rx").start(() -> {
            byte[] buf = new byte[8192];
            DatagramPacket datagram = new DatagramPacket(buf, buf.length);

            while (running.get()) {
                try {
                    socket.receive(datagram);
                    long recvTime = System.currentTimeMillis();

                    byte[] data = new byte[datagram.getLength()];
                    System.arraycopy(datagram.getData(), 0, data, 0, datagram.getLength());

                    Packet packet = CODEC.decode(data);

                    if (packet.opcode() == Opcode.ACK) {
                        System.out.println("✅ [Server] Joined session '" + SESSION_ID + "' successfully!");
                        System.out.println("💬 Type a message below and press ENTER to broadcast to your Android phone:\n");
                        continue;
                    }

                    if (packet.opcode() == Opcode.BROADCAST || packet.opcode() == Opcode.BROADCAST_ALL) {
                        String raw = new String(packet.payload(), StandardCharsets.UTF_8);
                        String[] parts = raw.split("\\|", 6);
                        if (parts.length >= 6 && "[CHAT]".equals(parts[0])) {
                            String sender = parts[1];
                            long sentTime = Long.parseLong(parts[4]);
                            String text = parts[5];
                            long latency = Math.max(0, recvTime - sentTime);
                            System.out.printf("📱 [%s] (%d ms): %s%n", sender, latency, text);
                        } else {
                            System.out.println("📥 [Incoming Broadcast]: " + raw);
                        }
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        System.err.println("Receive error: " + e.getMessage());
                    }
                    break;
                }
            }
        });

        // 3. User console input loop
        Scanner scanner = new Scanner(System.in);
        while (running.get() && scanner.hasNextLine()) {
            String line = scanner.nextLine().trim();
            if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) {
                break;
            }
            if (line.isEmpty()) continue;

            long now = System.currentTimeMillis();
            String chatPayload = "[CHAT]|" + USER_ID + "|" + SESSION_ID + "|1|" + now + "|" + line;
            byte[] payloadBytes = chatPayload.getBytes(StandardCharsets.UTF_8);

            Packet broadcast = Packet.ofBroadcast(SESSION_ID, payloadBytes);
            byte[] packetBytes = CODEC.encode(broadcast);

            socket.send(new DatagramPacket(packetBytes, packetBytes.length, serverAddress));
            System.out.println("You: " + line);
        }

        running.set(false);
        socket.close();
        System.out.println("Desktop peer stopped.");
    }
}
