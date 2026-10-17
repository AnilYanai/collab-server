package com.anilyanai.peer;

import com.anilyanai.business.security.HmacTokenAuthorizer;
import com.anilyanai.protocol.Opcode;
import com.anilyanai.protocol.Packet;
import com.anilyanai.protocol.codec.BinaryPacketCodec;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * =========================================================================================
 * 🎥 Interactive Desktop Video Call Peer (With Swing GUI)
 * =========================================================================================
 *
 * <p>Pairs with your Android Phone in the exact same session: <b>mobile-room-1</b>.</p>
 * <ul>
 *   <li>Renders the live video stream from your Android camera in real time!</li>
 *   <li>Transmits a live animated video feed from your PC back to your Android phone!</li>
 *   <li>Tracks latency (ms), frame sizes (KB), and framerate (FPS).</li>
 * </ul>
 */
public class DesktopVideoPeer {

    private static final String HOST = "localhost";
    private static final int PORT = 8080;
    private static final String SESSION_ID = "mobile-room-1";
    private static final String USER_ID = "DesktopPC";
    private static final String SHARED_SECRET = "collab-benchmark-secret-key-256bit-ok";

    private static final BinaryPacketCodec CODEC = new BinaryPacketCodec();
    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final AtomicInteger frameSeq = new AtomicInteger(0);

    // GUI Components
    private static JFrame frame;
    private static JLabel videoLabel;
    private static JLabel statusLabel;

    public static void main(String[] args) throws Exception {
        SwingUtilities.invokeLater(DesktopVideoPeer::setupGUI);

        InetSocketAddress serverAddress = new InetSocketAddress(HOST, PORT);
        DatagramSocket socket = new DatagramSocket();
        socket.setReceiveBufferSize(1024 * 1024);
        socket.setSendBufferSize(1024 * 1024);

        System.out.println("=================================================================");
        System.out.println("🎥 COLLAB VIDEO CALL DESKTOP PEER");
        System.out.println("=================================================================");
        System.out.println("Connecting to UDP: " + HOST + ":" + PORT);
        System.out.println("Session / Room   : " + SESSION_ID);
        System.out.println("User ID          : " + USER_ID);
        System.out.println("=================================================================\n");

        // 1. Send HMAC JOIN_SESSION
        String token = HmacTokenAuthorizer.generateToken(SESSION_ID, USER_ID, 3600, SHARED_SECRET);
        byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_8);

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

        // 2. Inbound Video & Chat Receiver Thread
        Thread.ofVirtual().name("desktop-video-rx").start(() -> {
            byte[] buf = new byte[65535];
            DatagramPacket datagram = new DatagramPacket(buf, buf.length);

            long lastFpsCalc = System.currentTimeMillis();
            int fpsCounter = 0;

            while (running.get()) {
                try {
                    socket.receive(datagram);
                    long recvTime = System.currentTimeMillis();

                    byte[] data = new byte[datagram.getLength()];
                    System.arraycopy(datagram.getData(), 0, data, 0, datagram.getLength());

                    Packet packet = CODEC.decode(data);

                    if (packet.opcode() == Opcode.ACK) {
                        System.out.println("✅ Joined room '" + SESSION_ID + "'. Video stream active!");
                        SwingUtilities.invokeLater(() -> statusLabel.setText("Status: Connected to " + SESSION_ID));
                        continue;
                    }

                    if (packet.opcode() == Opcode.BROADCAST || packet.opcode() == Opcode.BROADCAST_ALL) {
                        byte[] payload = packet.payload();
                        if (payload.length > 13 && payload[0] == 0x02) {
                            // Binary Video Frame
                            ByteBuffer bb = ByteBuffer.wrap(payload, 1, payload.length - 1);
                            long sentTime = bb.getLong();
                            int seq = bb.getInt();
                            long latency = Math.max(0, recvTime - sentTime);

                            byte[] jpeg = new byte[payload.length - 13];
                            bb.get(jpeg);

                            BufferedImage img = ImageIO.read(new ByteArrayInputStream(jpeg));
                            if (img != null) {
                                fpsCounter++;
                                long now = System.currentTimeMillis();
                                if (now - lastFpsCalc >= 1000) {
                                    int fps = fpsCounter;
                                    fpsCounter = 0;
                                    lastFpsCalc = now;
                                    SwingUtilities.invokeLater(() -> statusLabel.setText(
                                            String.format("Phone Camera | %d KB | Latency: %d ms | %d FPS",
                                                    jpeg.length / 1024, latency, fps)
                                    ));
                                }

                                // Scale to window size (480x360)
                                Image scaled = img.getScaledInstance(480, 360, Image.SCALE_FAST);
                                ImageIcon icon = new ImageIcon(scaled);
                                SwingUtilities.invokeLater(() -> videoLabel.setIcon(icon));
                            }
                        } else {
                            String text = new String(payload, StandardCharsets.UTF_8);
                            System.out.println("💬 [Chat]: " + text);
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

        // 3. Outbound PC Video Generator (Streams animated test feed back to Android phone)
        Thread.ofVirtual().name("desktop-video-tx").start(() -> {
            int ballX = 20;
            int ballY = 20;
            int dx = 4;
            int dy = 3;
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

            BufferedImage pcFrame = new BufferedImage(320, 240, BufferedImage.TYPE_INT_RGB);

            while (running.get()) {
                try {
                    Graphics2D g = pcFrame.createGraphics();
                    g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

                    // Background Gradient
                    GradientPaint gp = new GradientPaint(0, 0, new Color(15, 23, 42), 320, 240, new Color(30, 41, 59));
                    g.setPaint(gp);
                    g.fillRect(0, 0, 320, 240);

                    // Bouncing animated ball
                    ballX += dx;
                    ballY += dy;
                    if (ballX < 15 || ballX > 300) dx = -dx;
                    if (ballY < 15 || ballY > 220) dy = -dy;

                    g.setColor(new Color(56, 189, 248));
                    g.fillOval(ballX - 12, ballY - 12, 24, 24);

                    // Text overlay
                    g.setColor(Color.WHITE);
                    g.setFont(new Font("SansSerif", Font.BOLD, 14));
                    g.drawString("💻 Desktop PC Live Stream", 20, 35);

                    g.setColor(new Color(16, 185, 129));
                    g.setFont(new Font("Monospaced", Font.PLAIN, 12));
                    g.drawString("Time: " + LocalTime.now().format(dtf), 20, 60);
                    g.drawString("Room: " + SESSION_ID, 20, 80);
                    g.dispose();

                    // Compress to JPEG
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ImageIO.write(pcFrame, "jpg", baos);
                    byte[] jpegBytes = baos.toByteArray();

                    // Pack into video datagram
                    long now = System.currentTimeMillis();
                    int seq = frameSeq.incrementAndGet();

                    ByteBuffer buffer = ByteBuffer.allocate(1 + 8 + 4 + jpegBytes.length);
                    buffer.put((byte) 0x02); // TAG_VIDEO
                    buffer.putLong(now);
                    buffer.putInt(seq);
                    buffer.put(jpegBytes);

                    Packet packet = Packet.ofBroadcast(SESSION_ID, buffer.array());
                    byte[] raw = CODEC.encode(packet);

                    socket.send(new DatagramPacket(raw, raw.length, serverAddress));

                    // ~15 FPS (66 ms sleep)
                    Thread.sleep(66);
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    // Ignore transient errors
                }
            }
        });
    }

    private static void setupGUI() {
        frame = new JFrame("🎥 Collab Live Video Feed (mobile-room-1)");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(520, 460);
        frame.setLocationRelativeTo(null);
        frame.getContentPane().setBackground(new Color(15, 23, 42));
        frame.setLayout(new BorderLayout(10, 10));

        statusLabel = new JLabel("Status: Connecting to server...", SwingConstants.CENTER);
        statusLabel.setForeground(new Color(56, 189, 248));
        statusLabel.setFont(new Font("SansSerif", Font.BOLD, 13));
        statusLabel.setBorder(BorderFactory.createEmptyBorder(10, 10, 5, 10));
        frame.add(statusLabel, BorderLayout.NORTH);

        videoLabel = new JLabel("Waiting for Android Phone Camera...", SwingConstants.CENTER);
        videoLabel.setForeground(Color.GRAY);
        videoLabel.setPreferredSize(new Dimension(480, 360));
        videoLabel.setBackground(Color.BLACK);
        videoLabel.setOpaque(true);
        frame.add(videoLabel, BorderLayout.CENTER);

        frame.setVisible(true);
    }
}
