package com.anilyanai.connector.events;

public interface InboundMessageHandler {
    void onReceive(
            String connectionId,
            byte[] data
    );

}
