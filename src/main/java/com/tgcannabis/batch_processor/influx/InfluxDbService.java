package com.tgcannabis.batch_processor.influx;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.client.write.events.WriteErrorEvent;
import com.influxdb.client.write.events.WriteSuccessEvent;
import com.influxdb.exceptions.InfluxException;
import com.tgcannabis.batch_processor.config.BatchProcessorConfig;
import com.tgcannabis.batch_processor.model.SensorData; // Assuming model location
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;

/**
 * Handles writing sensor data points to InfluxDB.
 * Uses the non-blocking Write API with background flushing and error handling.
 */
public class InfluxDbService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDbService.class);

    private final BatchProcessorConfig config;

    @Setter
    private InfluxDBClient influxDBClient;

    @Setter
    private WriteApi writeApi; // Non-blocking API

    /**
     * Constructs the InfluxDB Service.
     *
     * @param config The application configuration. Must not be null.
     */
    public InfluxDbService(BatchProcessorConfig config) {
        this.config = Objects.requireNonNull(config, "Configuration cannot be null");
        initializeClient();
    }

    public InfluxDbService(BatchProcessorConfig config, InfluxDBClient client, WriteApi writeApi) {
        this.config = config;
        this.influxDBClient = client;
        this.writeApi = writeApi;
    }

    /**
     * Initializes the InfluxDB client and the non-blocking Write API.
     */
    protected void initializeClient() {
        try {
            LOGGER.info("Initializing InfluxDB Client for URL: {}", config.getInfluxUrl());
            influxDBClient = InfluxDBClientFactory.create(
                    config.getInfluxUrl(),
                    config.getInfluxToken().toCharArray(),
                    config.getInfluxOrg(),
                    config.getInfluxBucket()
            );

            // Check connection - ping throws exception on failure
            influxDBClient.ping();
            LOGGER.info("InfluxDB connection successful (ping ok).");

            // Setup non-blocking Write API with error handling
            // Records are buffered and written in batches in the background
            writeApi = influxDBClient.makeWriteApi();
            writeApi.listenEvents(WriteErrorEvent.class, event -> {
                LOGGER.error("Error writing to InfluxDB (non-blocking API): ", event.getThrowable());
                // Implement strategy for failed writes if needed (e.g., logging, metrics)
            });
            writeApi.listenEvents(WriteSuccessEvent.class, event -> {
                // Optional: Log successful batch writes if needed for debugging
                LOGGER.debug("Successfully wrote batch to InfluxDB: {}", event.getLineProtocol());
            });


        } catch (InfluxException e) {
            LOGGER.error("Failed to initialize InfluxDB Client or ping failed: {}", e.getMessage(), e);
            influxDBClient = null; // Ensure client is null if init failed
            writeApi = null;
            // Depending on requirements, could throw exception to halt startup
        }
    }

    /**
     * Writes sensor data to InfluxDB using the non-blocking API.
     * The data point is added to a buffer and written in the background.
     * Does nothing if the client failed to initialize.
     *
     * @param data             The SensorData object to write. Must not be null.
     * @param originatingTopic The MQTT topic the data came from (used as a tag). Can be null.
     */
    public void writeSensorData(SensorData data, String originatingTopic) {
        Objects.requireNonNull(data, "SensorData cannot be null");

        // Check for uninitialized client
        if (this.writeApi == null || this.influxDBClient == null) {
            LOGGER.warn("InfluxDB client/write API not initialized. Attempting to reinitialize...");
            initializeClient();
            if (this.writeApi == null || this.influxDBClient == null) {
                throw new IllegalStateException("InfluxDB client could not be initialized");
            }
        }

        if (data.getSensorId() == null || data.getSensorType() == null) {
            LOGGER.warn("Incomplete SensorData received, skipping InfluxDB write: {}", data);
            return;
        }

        try {
            Point point = Point.measurement(data.getSensorType())
                    .addTag("sensorId", data.getSensorId())
                    .addTag("location", data.getLocation() != null ? data.getLocation() : "unknown")
                    .addTag("originTopic", originatingTopic != null ? originatingTopic : "unknown")
                    .addTag("sensorType", data.getSensorType())
                    .addField("value", data.getValue())
                    .addField("timestamp", data.getTimestamp())
                    .time(Instant.ofEpochMilli(data.getTimestamp()), WritePrecision.MS);

            LOGGER.debug("Queueing point for InfluxDB: {}", point.toLineProtocol());
            writeApi.writePoint(point);

        } catch (Exception e) {
            LOGGER.error("Error creating InfluxDB Point object: {}", e.getMessage(), e);
        }
    }


    /**
     * Closes the InfluxDB client and Write API gracefully.
     * This ensures any buffered points are flushed.
     */
    @Override
    public void close() {
        if (influxDBClient != null) {
            LOGGER.info("Closing InfluxDB client (flushing writes)...");
            // writeApi.close() is implicitly called by influxDBClient.close()
            influxDBClient.close();
            LOGGER.info("InfluxDB client closed.");
            influxDBClient = null;
            writeApi = null;
        }
    }
}