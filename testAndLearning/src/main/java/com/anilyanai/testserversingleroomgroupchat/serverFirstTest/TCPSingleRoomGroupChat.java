package com.anilyanai.testserversingleroomgroupchat.serverFirstTest;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class TCPSingleRoomGroupChat {

    private static final String HOST = "localhost";
    private static final int PORT = 8080;

    public static void main(String[] args) {
        System.out.println("Connecting to " + HOST + ":" + PORT + "...");

        try (Socket socket = new Socket(HOST, PORT);
             BufferedReader serverReader = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter serverWriter = new PrintWriter(
                     new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
             BufferedReader console = new BufferedReader(
                     new InputStreamReader(System.in, StandardCharsets.UTF_8))) {

            System.out.println("Connected to " + HOST + ":" + PORT);
            System.out.println("Type your message and press Enter (type '/exit' or 'quit' to disconnect):");

            // Background receiver thread
            Thread receiver = new Thread(() -> {
                try {
                    String message;
                    while ((message = serverReader.readLine()) != null) {
                        System.out.println("\n[SERVER] " + message);
                        System.out.print("> ");
                    }
                    System.out.println("\nServer closed connection.");
                } catch (IOException e) {
                    if (!socket.isClosed()) {
                        System.out.println("\nConnection error: " + e.getMessage());
                    }
                }
            });

            receiver.setDaemon(true);
            receiver.start();

            // Main sender loop
            System.out.print("> ");
            String message;
            while ((message = console.readLine()) != null) {
                if ("quit".equalsIgnoreCase(message.trim()) || "/exit".equalsIgnoreCase(message.trim())) {
                    System.out.println("Disconnecting...");
                    break;
                }

                serverWriter.println(message);
                System.out.print("> ");
            }

        } catch (IOException e) {
            System.err.println("Client exception: " + e.getMessage());
        }
    }
}
