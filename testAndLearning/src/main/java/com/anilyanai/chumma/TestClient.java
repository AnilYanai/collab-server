package com.anilyanai.chumma;

import java.io.OutputStream;
import java.net.Socket;

public class TestClient {

    public static void main(String[] args)
            throws Exception {
        Socket socket =
                new Socket(
                        "localhost",
                        8080
                );


        System.out.println("Connected");


        OutputStream out =
                socket.getOutputStream();


        int count = 1;


        while(true) {

            String message =
                    "hello " + count;


            out.write(
                    message.getBytes()
            );


            out.flush();


            System.out.println(
                    "Sent: " + message
            );


            count++;


            Thread.sleep(1000);
        }
    }
}

