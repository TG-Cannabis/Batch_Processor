package com.tgcannabis.batch_processor;

import com.tgcannabis.batch_processor.config.BatchProcessorConfig;
import com.tgcannabis.batch_processor.influx.InfluxDbService;
import com.tgcannabis.batch_processor.kafka.KafkaService;
import com.tgcannabis.batch_processor.mqtt.MqttService;
import com.tgcannabis.batch_processor.processing.SensorDataHandler;
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

    private final BatchProcessorConfig config;

    public BatchProcessorApplication() {
        this.config = new BatchProcessorConfig();
    }

    public BatchProcessorApplication(BatchProcessorConfig config,
                                     KafkaService kafkaService,
                                     InfluxDbService influxDbService,
                                     MqttService mqttService) {
        this.config = config;
        this.kafkaService = kafkaService;
        this.influxDbService = influxDbService;
        this.mqttService = mqttService;
    }

    /**
     * Starts the batch processor application.
     */
    public void start() {
        LOGGER.info("Starting IoT Batch Processor Application...");

        try {
            // 1. Initialize Services
            if (kafkaService == null) {
                kafkaService = new KafkaService(config);
            }
            if (influxDbService == null) {
                influxDbService = new InfluxDbService(config);
            }
            if (mqttService == null) {
                mqttService = new MqttService(config);
            }

            // 2. Create and Wire Handler
            SensorDataHandler messageHandler = new SensorDataHandler(kafkaService, influxDbService);
            mqttService.setMessageHandler(messageHandler); // Set the handler in MqttService

            // 3. Connect MQTT (which will trigger subscription)
            mqttService.connect(); // Handle potential MqttException

            // 4. Add Shutdown Hook for graceful cleanup
            addShutdownHook();

            LOGGER.info("Batch Processor Application started successfully.");

            // Keep the main thread alive (alternative: use a CountDownLatch or CompletableFuture)
            Thread.currentThread().join();

        } catch (Exception e) {
            LOGGER.error("FATAL: Application failed to start.", e);
            // Ensure cleanup even if startup fails partially
            shutdown();
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
            try {
                mqttService.close();
            } catch (Exception e) {
                LOGGER.error("Error closing MQTT Service", e);
            }
        }
        if (kafkaService != null) {
            try {
                kafkaService.close();
            } catch (Exception e) {
                LOGGER.error("Error closing Kafka Service", e);
            }
        }
        if (influxDbService != null) {
            try {
                influxDbService.close();
            } catch (Exception e) {
                LOGGER.error("Error closing InfluxDB Service", e);
            }
        }
        LOGGER.info("Batch Processor Application shut down complete.");
    }


    /**
     * Main method. Creates an instance of the application and starts it.
     *
     * @param args Command line arguments (not used).
     */
    public static void main(String[] args) {
        BatchProcessorApplication app = new BatchProcessorApplication();
        app.start();
    }
}