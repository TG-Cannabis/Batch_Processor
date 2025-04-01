package com.tgcannabis.batchprocessor.mqtt;


import com.tgcannabis.batchprocessor.config.BatchProcessorConfig;
import lombok.Setter;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Handles connection, subscription, and message reception from the MQTT broker.
 */
public class MqttService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttService.class);

    private final BatchProcessorConfig config;
    private MqttClient mqttClient;
    /**
     * -- SETTER --
     *  Sets the handler to be called when an MQTT message arrives.
     *
     * @param messageHandler A BiConsumer accepting Topic (String) and Payload (String).
     */
    @Setter
    private BiConsumer<String, String> messageHandler; // Functional interface for message handling

    /**
     * Constructs the MQTT Service.
     *
     * @param config The application configuration. Must not be null.
     */
    public MqttService(BatchProcessorConfig config) {
        this.config = Objects.requireNonNull(config, "Configuration cannot be null");
    }

    /**
     * Connects to the MQTT broker and sets up the callback for message handling.
     *
     * @throws MqttException if connection fails.
     */
    public void connect() throws MqttException {
        Objects.requireNonNull(messageHandler, "Message handler must be set before connecting.");

        mqttClient = new MqttClient(config.getMqttBroker(), config.getMqttClientId(), new MemoryPersistence());
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setAutomaticReconnect(true);
        connOpts.setConnectionTimeout(10); // seconds
        connOpts.setKeepAliveInterval(20); // seconds

        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                LOGGER.info("MQTT Connection {}complete to {}", (reconnect ? "re" : ""), serverURI);
                subscribe(); // Subscribe/resubscribe after connection is established
            }

            @Override
            public void connectionLost(Throwable cause) {
                LOGGER.warn("MQTT Connection lost!", cause);
                // Automatic reconnect should handle this if enabled.
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                try {
                    String payload = new String(message.getPayload());
                    LOGGER.debug("MQTT Message received - Topic: [{}], Payload: [{}]", topic, payload);
                    if (messageHandler != null) {
                        messageHandler.accept(topic, payload);
                    } else {
                        LOGGER.warn("No message handler set for received message on topic {}", topic);
                    }
                } catch (Exception e) {
                    // Catch exceptions from the handler to prevent Paho callback thread death
                    LOGGER.error("Error processing message from topic {}: {}", topic, e.getMessage(), e);
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // Not used in this subscriber-focused service
            }
        });

        LOGGER.info("Connecting to MQTT broker: {}", config.getMqttBroker());
        mqttClient.connect(connOpts);
    }

    /**
     * Subscribes to the topic defined in the configuration.
     * Should be called after a successful connection or reconnection.
     */
    private void subscribe() {
        if (mqttClient != null && mqttClient.isConnected()) {
            try {
                String topicFilter = config.getMqttTopicFilter();
                LOGGER.info("Subscribing to MQTT topic filter: {}", topicFilter);
                mqttClient.subscribe(topicFilter, 1); // QoS 1: At least once
            } catch (MqttException e) {
                LOGGER.error("Error subscribing to MQTT topic filter '{}': {}", config.getMqttTopicFilter(), e.getMessage(), e);
                // Consider retry logic or application shutdown depending on severity
            }
        } else {
            LOGGER.warn("Cannot subscribe, MQTT client not connected.");
        }
    }

    /**
     * Disconnects the MQTT client gracefully.
     */
    @Override
    public void close() {
        if (mqttClient != null && mqttClient.isConnected()) {
            try {
                LOGGER.info("Disconnecting MQTT client...");
                mqttClient.disconnect();
                LOGGER.info("MQTT client disconnected successfully.");
            } catch (MqttException e) {
                LOGGER.error("Error disconnecting MQTT client: {}", e.getMessage(), e);
            } finally {
                closeClientQuietly();
            }
        } else if (mqttClient != null) {
            closeClientQuietly();
        }
    }

    /** Closes the underlying client instance, suppressing exceptions. */
    private void closeClientQuietly() {
        try {
            mqttClient.close();
        } catch (MqttException e) {
            LOGGER.error("Error closing MQTT client instance: {}", e.getMessage(), e);
        } finally {
            mqttClient = null;
        }
    }
}