package com.tgcannabis.batch_processor.processing;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.tgcannabis.batch_processor.influx.InfluxDbService;
import com.tgcannabis.batch_processor.kafka.KafkaService;
import com.tgcannabis.batch_processor.model.SensorData; // Assuming model location
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Handles incoming MQTT messages containing sensor data.
 * It deserializes the payload, attempts to send it to Kafka,
 * and writes it to InfluxDB.
 */
public class SensorDataHandler implements BiConsumer<String, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SensorDataHandler.class);
    private static final Gson gson = new Gson(); // Thread-safe

    private final KafkaService kafkaService;
    private final InfluxDbService influxDbService;

    /**
     * Constructs the message handler.
     *
     * @param kafkaService    Service for publishing to Kafka. Must not be null.
     * @param influxDbService Service for writing to InfluxDB. Must not be null.
     */
    public SensorDataHandler(KafkaService kafkaService, InfluxDbService influxDbService) {
        this.kafkaService = Objects.requireNonNull(kafkaService, "KafkaService cannot be null");
        this.influxDbService = Objects.requireNonNull(influxDbService, "InfluxDbService cannot be null");
    }

    /**
     * Processes an incoming MQTT message payload.
     * This method implements the BiConsumer interface for use with MqttService.
     *
     * @param topic   The MQTT topic the message arrived at.
     * @param payload The raw message payload (expected to be JSON).
     */
    @Override
    public void accept(String topic, String payload) {
        LOGGER.debug("Processing message - Topic: [{}], Payload: [{}]", topic, payload);
        try {
            // 1. Deserialize JSON
            SensorData sensorData = gson.fromJson(payload, SensorData.class);

            // Basic validation
            if (sensorData == null || sensorData.getSensorId() == null) {
                LOGGER.warn("Skipping message due to incomplete data after deserialization: {}", payload);
                return;
            }

            // Use sensor ID as Kafka key for potential partitioning
            String kafkaKey = sensorData.getSensorId();

            // 2. Attempt to send raw JSON payload to Kafka
            // KafkaService handles async send and logging internally
            kafkaService.sendMessage(kafkaKey, payload);

            // 3. Write deserialized data to InfluxDB
            // InfluxDbService handles async write and logging internally
            influxDbService.writeSensorData(sensorData, topic);

        } catch (JsonSyntaxException e) {
            LOGGER.error("JSON Parsing Error - Topic: [{}], Payload: [{}], Error: {}", topic, payload, e.getMessage());
            // Optionally send malformed messages to a dead-letter topic/queue
        } catch (Exception e) {
            LOGGER.error("Unexpected error processing message - Topic: [{}], Error: {}", topic, e.getMessage(), e);
        }
    }
}