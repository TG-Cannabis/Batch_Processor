package com.tgcannabis.batch_processor.kafka;

import com.tgcannabis.batch_processor.config.BatchProcessorConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class KafkaServiceTest {

    private BatchProcessorConfig config;
    private KafkaProducer<String, String> mockProducer;

    @BeforeEach
    void setUp() {
        config = mock(BatchProcessorConfig.class);
        when(config.getKafkaBrokers()).thenReturn("localhost:9093");
        when(config.getKafkaTopic()).thenReturn("test-topic");
        when(config.getKafkaClientId()).thenReturn("test-client");

        mockProducer = mock(KafkaProducer.class);
    }

    @Test
    void shouldThrowIfValueIsNull() {
        KafkaService service = new KafkaService(config);
        assertThrows(NullPointerException.class, () -> service.sendMessage("key", null));
    }

    @Test
    void shouldWarnIfProducerIsNotInitialized() {
        BatchProcessorConfig badConfig = mock(BatchProcessorConfig.class);

        when(badConfig.getKafkaBrokers()).thenReturn(null);
        when(badConfig.getKafkaClientId()).thenReturn("test-client");
        when(badConfig.getKafkaTopic()).thenReturn("topic");

        KafkaService service = new KafkaService(badConfig);

        assertThrows(NullPointerException.class, () -> service.sendMessage("key", "value"));
    }

    @Test
    void shouldSendMessageWhenProducerIsAvailable() {
        KafkaService service = new KafkaService(config, mockProducer);
        service.sendMessage("key", "value");

        verify(mockProducer).send(any(ProducerRecord.class), any(Callback.class));
    }

    @Test
    void shouldCloseProducerGracefully() {
        KafkaService service = new KafkaService(config, mockProducer);
        service.close();

        verify(mockProducer).close(Duration.ofSeconds(10));
    }

    @Test
    void shouldNotFailOnDoubleClose() {
        KafkaService service = new KafkaService(config, mockProducer);

        service.close(); // First close
        service.close(); // Should not throw or call close() again
        verify(mockProducer, times(1)).close(Duration.ofSeconds(10));
    }
}
