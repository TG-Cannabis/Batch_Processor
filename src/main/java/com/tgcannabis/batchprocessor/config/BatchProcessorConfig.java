package com.tgcannabis.batchprocessor.config;

import io.github.cdimascio.dotenv.Dotenv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Loads and holds configuration parameters for the Batch Processor application.
 * Reads configuration from environment variables or a .env file.
 */
public class BatchProcessorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchProcessorConfig.class);

    // MQTT Configuration
    private final String mqttBroker;
    private final String mqttClientId;
    private final String mqttTopicFilter;

    // InfluxDB Configuration
    private final String influxUrl;
    private final String influxToken;
    private final String influxOrg;
    private final String influxBucket;

    // Kafka Configuration
    private final String kafkaBrokers;
    private final String kafkaTopic;
    private final String kafkaClientId;

    /**
     * Loads configuration using Dotenv library, looking for a .env file
     * in the classpath or project root, and falling back to environment variables.
     */
    public BatchProcessorConfig() {
        // Configure Dotenv to search in standard places and ignore missing file
        Dotenv dotenv = Dotenv.configure()
                .ignoreIfMissing() // Don't fail if .env is not present
                .load();

        // Load MQTT settings
        mqttBroker = getEnv(dotenv, "MQTT_BROKER", "tcp://localhost:1883");
        mqttClientId = getEnv(dotenv, "MQTT_CLIENT_ID", "batch-processor-" + System.currentTimeMillis());
        mqttTopicFilter = getEnv(dotenv, "MQTT_TOPIC_FILTER", "sensors/#");

        // Load InfluxDB settings
        influxUrl = getEnv(dotenv, "INFLUX_URL", "http://localhost:8086");
        influxToken = getEnvOrThrow(dotenv, "INFLUX_TOKEN", "InfluxDB write token is required.");
        influxOrg = getEnvOrThrow(dotenv, "INFLUX_ORG", "InfluxDB organization is required.");
        influxBucket = getEnvOrThrow(dotenv, "INFLUX_BUCKET", "InfluxDB bucket name is required.");

        // Load Kafka settings
        kafkaBrokers = getEnv(dotenv, "KAFKA_BROKERS", "localhost:9092");
        kafkaTopic = getEnv(dotenv, "KAFKA_TOPIC", "sensores_cloud");
        kafkaClientId = getEnv(dotenv, "KAFKA_CLIENT_ID", "batch-processor-kafka-client");

        logConfiguration();
    }

    // --- Getters ---

    public String getMqttBroker() { return mqttBroker; }
    public String getMqttClientId() { return mqttClientId; }
    public String getMqttTopicFilter() { return mqttTopicFilter; }
    public String getInfluxUrl() { return influxUrl; }
    public String getInfluxToken() { return influxToken; }
    public String getInfluxOrg() { return influxOrg; }
    public String getInfluxBucket() { return influxBucket; }
    public String getKafkaBrokers() { return kafkaBrokers; }
    public String getKafkaTopic() { return kafkaTopic; }
    public String getKafkaClientId() { return kafkaClientId; }


    // --- Helper Methods ---

    /**
     * Gets a value from Dotenv or system environment, returning a default if not found.
     * @param dotenv Dotenv instance
     * @param varName Environment variable name
     * @param defaultValue Default value if not found
     * @return The value found or the default value
     */
    private String getEnv(Dotenv dotenv, String varName, String defaultValue) {
        String value = dotenv.get(varName);
        if (value == null || value.trim().isEmpty()) {
            LOGGER.warn("Environment variable '{}' not found or empty, using default: '{}'", varName, defaultValue);
            return defaultValue;
        }
        return value;
    }

    /**
     * Gets a value from Dotenv or system environment, throwing an exception if not found.
     * @param dotenv Dotenv instance
     * @param varName Environment variable name
     * @param errorMessage Error message if not found
     * @return The value found
     * @throws IllegalStateException if the variable is missing or empty
     */
    private String getEnvOrThrow(Dotenv dotenv, String varName, String errorMessage) {
        String value = dotenv.get(varName);
        if (value == null || value.trim().isEmpty()) {
            LOGGER.error("Missing required environment variable: '{}'. {}", varName, errorMessage);
            throw new IllegalStateException("Missing required environment variable: " + varName);
        }
        return value;
    }

    /** Logs the loaded configuration (except sensitive tokens). */
    private void logConfiguration() {
        LOGGER.info("Batch Processor Configuration Loaded:");
        LOGGER.info("  MQTT Broker: {}", mqttBroker);
        LOGGER.info("  MQTT Client ID: {}", mqttClientId);
        LOGGER.info("  MQTT Topic Filter: {}", mqttTopicFilter);
        LOGGER.info("  InfluxDB URL: {}", influxUrl);
        LOGGER.info("  InfluxDB Org: {}", influxOrg);
        LOGGER.info("  InfluxDB Bucket: {}", influxBucket);
        LOGGER.info("  InfluxDB Token: {}", (influxToken != null && !influxToken.isEmpty()) ? "****" : "Not Set");
        LOGGER.info("  Kafka Brokers: {}", kafkaBrokers);
        LOGGER.info("  Kafka Topic: {}", kafkaTopic);
        LOGGER.info("  Kafka Client ID: {}", kafkaClientId);
    }
}