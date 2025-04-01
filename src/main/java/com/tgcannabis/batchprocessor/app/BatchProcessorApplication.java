package com.tgcannabis.batchprocessor.app;


import com.tgcannabis.batchprocessor.config.BatchProcessorConfig;
import com.tgcannabis.batchprocessor.influx.InfluxDbService;
import com.tgcannabis.batchprocessor.kafka.KafkaService;
import com.tgcannabis.batchprocessor.mqtt.MqttService;
import com.tgcannabis.batchprocessor.processing.SensorDataHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the IoT Batch Processor application.
 * Initializes configuration, services, wires them together, and handles lifecycle.
 */
public class BatchProcessorApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchProcessorApplication.class);

    private MqttService mqttService;
    private KafkaService kafkaService;
    private InfluxDbService influxDbService;

    /**
     * Starts the batch processor application.
     */
    public void start() {
        LOGGER.info("Starting IoT Batch Processor Application...");

        try {
            // 1. Load Configuration
            BatchProcessorConfig config = new BatchProcessorConfig();

            // 2. Initialize Services
            kafkaService = new KafkaService(config);
            influxDbService = new InfluxDbService(config);
            mqttService = new MqttService(config);

            // 3. Create and Wire Handler
            SensorDataHandler messageHandler = new SensorDataHandler(kafkaService, influxDbService);
            mqttService.setMessageHandler(messageHandler); // Set the handler in MqttService

            // 4. Connect MQTT (which will trigger subscription)
            mqttService.connect(); // Handle potential MqttException

            // 5. Add Shutdown Hook for graceful cleanup
            addShutdownHook();

            LOGGER.info("Batch Processor Application started successfully.");

            // Keep the main thread alive (alternative: use a CountDownLatch or CompletableFuture)
            Thread.currentThread().join();

        } catch (Exception e) {
            LOGGER.error("FATAL: Application failed to start.", e);
            // Ensure cleanup even if startup fails partially
            shutdown();
            System.exit(1); // Exit with error code
        }
    }

    /**
     * Registers a JVM shutdown hook to gracefully close resources.
     */
    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown hook triggered. Cleaning up resources...");
            this.shutdown();
            LOGGER.info("Cleanup finished.");
        }));
    }

    /**
     * Gracefully shuts down all services.
     */
    public void shutdown() {
        LOGGER.info("Shutting down Batch Processor Application...");
        // Close in reverse order of dependency or where it makes sense
        if (mqttService != null) {
            try { mqttService.close(); } catch (Exception e) { LOGGER.error("Error closing MQTT Service", e); }
        }
        if (kafkaService != null) {
            try { kafkaService.close(); } catch (Exception e) { LOGGER.error("Error closing Kafka Service", e); }
        }
        if (influxDbService != null) {
            try { influxDbService.close(); } catch (Exception e) { LOGGER.error("Error closing InfluxDB Service", e); }
        }
        LOGGER.info("Batch Processor Application shut down complete.");
    }


    /**
     * Main method. Creates an instance of the application and starts it.
     * @param args Command line arguments (not used).
     */
    public static void main(String[] args) {
        BatchProcessorApplication app = new BatchProcessorApplication();
        app.start();
    }
}