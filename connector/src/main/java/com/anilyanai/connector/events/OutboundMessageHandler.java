package com.anilyanai.connector.events;

public interface OutboundMessageHandler {

    byte[] onSend(
            String connectionId,
            Object data
    );
}
