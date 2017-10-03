package com.yahoo.bullet.storm.drpc;

import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.result.JSONFormatter;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DRPCMessage implements JSONFormatter {
    private PubSubMessage message;

    @Override
    public String asJSON() {
        return JSONFormatter.asJSON(message);
    }

    /**
     * Helper method to convert a {@link PubSubMessage} to JSON.
     *
     * @param message The message to convert.
     * @return A String representation of the JSON.
     */
    public static String toJSON(PubSubMessage message) {
        return new DRPCMessage(message).asJSON();
    }

    /**
     *
     * @param json A String json representing the {@link PubSubMessage}.
     *
     * @return An instance of the PubSubMessage.
     */
    public static PubSubMessage fromJSON(String json) {
        return GSON.fromJson(json, PubSubMessage.class);
    }
}
