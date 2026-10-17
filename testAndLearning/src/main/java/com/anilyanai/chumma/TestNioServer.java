package com.anilyanai.chumma;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;

public class TestNioServer {
    public static void main(String[] args)
            throws Exception {


        ServerSocketChannel server =
                ServerSocketChannel.open();


        server.bind(
                new InetSocketAddress(8080)
        );


        server.configureBlocking(false);


        Selector selector =
                Selector.open();


        server.register(
                selector,
                SelectionKey.OP_ACCEPT
        );


        System.out.println("Server started");


        while(true) {


            selector.select();
//            System.out.println("selector  selected");
            try {

                for (SelectionKey key :
                        selector.selectedKeys()) {

                    if(key.channel() instanceof ServerSocketChannel) {
                        System.out.println("selector wakeup on server socket channel ");
                    }
                    if(key.channel() instanceof SocketChannel) {
                        System.out.println("selector wakeup on socket channel ");
                    }

                    if (key.isAcceptable()) {

                        ServerSocketChannel serverChannel =
                                (ServerSocketChannel) key.channel();

//                        System.out.println("socket channel before");
                        SocketChannel client =
//                                (SocketChannel) key.channel();
                                serverChannel.accept();

                        System.out.println("after accept");

                        if (client != null) {

                            client.configureBlocking(false);


                            client.register(
                                    selector,
                                    SelectionKey.OP_READ
                            );


                            System.out.println(
                                    "Client connected: "
                                            + client
                            );
                        }
                        else {
                            System.out.println(
                                    "Client not connected: "
                                            + client
                            );
                        }
                    }


                    if (key.isReadable()) {


                        // THIS IS THE IMPORTANT PART

                        SocketChannel client =
                                (SocketChannel) key.channel();


                        System.out.println(
                                "Got SocketChannel:"
                                        + client
                        );


                        ByteBuffer buffer =
                                ByteBuffer.allocate(1024);


                        int read =
                                client.read(buffer);


                        if (read == -1) {

                            client.close();
                            continue;
                        }


                        buffer.flip();


                        byte[] data =
                                new byte[buffer.remaining()];


                        buffer.get(data);


                        System.out.println(
                                "Message: "
                                        + new String(data)
                        );

                    }


                }


                selector.selectedKeys().clear();
            } catch (Exception e) {
//                throw new RuntimeException(e);
                System.out.println(e);
            }
        }
    }
}
