package com.tgcannabis.batch_processor.kafka;


import com.tgcannabis.batch_processor.config.BatchProcessorConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

/**
 * Handles publishing messages to the configured Apache Kafka topic.
 * Provides asynchronous sending with logging for success or failure.
 */
public class KafkaService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaService.class);

    private final BatchProcessorConfig config;
    private KafkaProducer<String, String> producer;

    /**
     * Constructs the Kafka Service.
     *
     * @param config The application configuration. Must not be null.
     */
    public KafkaService(BatchProcessorConfig config) {
        this.config = Objects.requireNonNull(config, "Configuration cannot be null");
        initializeProducer();
    }

    public KafkaService(BatchProcessorConfig config, KafkaProducer<String, String> producer) {
        this.config = Objects.requireNonNull(config, "Configuration cannot be null");
        this.producer = producer;
    }

    /**
     * Initializes the KafkaProducer instance based on configuration.
     */
    private void initializeProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBrokers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getKafkaClientId());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Optional: Add more production-ready settings like acks, retries, timeouts
        // props.put(ProducerConfig.ACKS_CONFIG, "all"); // Higher durability
        // props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000); // Max time for delivery

        // --- NUEVOS PARÁMETROS PARA RECONEXIÓN ---
        // Espera base antes de reintentar la conexión (60000 ms = 1 minuto)
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "60000");
        // Espera máxima antes de reintentar la conexión (60000 ms = 1 minuto)
        props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "600000");
        // --- FIN NUEVOS PARÁMETROS ---

        try {
            LOGGER.info("Initializing Kafka Producer for brokers: {}", config.getKafkaBrokers());
            this.producer = new KafkaProducer<>(props);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize Kafka Producer: {}", e.getMessage(), e);
            // Depending on requirements, could throw exception to halt startup
            this.producer = null;
        }
    }

    /**
     * Sends a message asynchronously to the configured Kafka topic.
     * Logs the outcome (success or failure) via callback.
     * Does nothing if the producer failed to initialize.
     *
     * @param key   The key for the Kafka record (can be null).
     * @param value The value (message payload) for the Kafka record. Must not be null.
     */
    public void sendMessage(String key, String value) {
        Objects.requireNonNull(value, "Kafka message value cannot be null");
        if (this.producer == null) {
            LOGGER.warn("Kafka producer is not initialized. Cannot send message to topic '{}'", config.getKafkaTopic());
            // Optionally implement a retry mechanism or dead-letter queue here
            return; // Fail fast if producer isn't ready
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(config.getKafkaTopic(), key, value);
        LOGGER.debug("Attempting Kafka send: Topic=[{}], Key=[{}]", record.topic(), record.key());

        // Send asynchronously
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                // Success
                LOGGER.debug("Kafka send successful: Topic=[{}], Partition=[{}], Offset=[{}]",
                        metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                // Failure
                LOGGER.error("Kafka send failed: Topic=[{}], Key=[{}], Error: {}",
                        record.topic(), record.key(), exception.getMessage());
                // Consider logging exception stack trace at DEBUG or based on config
                // LOGGER.debug("Kafka send failure stack trace:", exception);
                // Implement retry logic or error handling strategy here if needed
            }
        });
    }

    /**
     * Closes the Kafka producer gracefully.
     */
    @Override
    public void close() {
        if (producer != null) {
            LOGGER.info("Closing Kafka producer...");
            // Flush any buffered records and close with a timeout
            producer.close(java.time.Duration.ofSeconds(10));
            LOGGER.info("Kafka producer closed.");
            producer = null;
        }
    }
}