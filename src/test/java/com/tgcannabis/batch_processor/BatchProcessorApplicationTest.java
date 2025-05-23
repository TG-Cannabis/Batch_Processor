package com.tgcannabis.batch_processor;

import com.tgcannabis.batch_processor.config.BatchProcessorConfig;
import com.tgcannabis.batch_processor.influx.InfluxDbService;
import com.tgcannabis.batch_processor.kafka.KafkaService;
import com.tgcannabis.batch_processor.mqtt.MqttService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

class BatchProcessorApplicationTest {
    private MqttService mockMqttService;
    private KafkaService mockKafkaService;
    private InfluxDbService mockInfluxService;

    private BatchProcessorApplication application;

    @BeforeEach
    void setUp() {
        BatchProcessorConfig mockConfig = mock(BatchProcessorConfig.class);
        mockKafkaService = mock(KafkaService.class);
        mockInfluxService = mock(InfluxDbService.class);
        mockMqttService = mock(MqttService.class);

        application = new BatchProcessorApplication(mockConfig, mockKafkaService, mockInfluxService, mockMqttService);
    }

    @Test
    void shouldStartApplicationAndConnectMqtt() throws Exception {
        Thread appThread = new Thread(() -> {
            try {
                application.start();
            } catch (Exception e) {
                System.err.println("Application thread caught an exception during startup: " + e.getMessage());
            }
        });

        appThread.start();
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(mockMqttService).setMessageHandler(any());
            verify(mockMqttService).connect();
        });

        application.shutdown();

        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(mockMqttService).close();
            verify(mockKafkaService).close();
            verify(mockInfluxService).close();
        });

        appThread.join(2000);
    }

    @Test
    void shouldShutdownServices() throws Exception {
        Thread appThread = new Thread(() -> {
            try {
                application.start();
            } catch (Exception e) {
                System.err.println("Application thread caught an unexpected exception during startup: " + e.getMessage());
            }
        });
        appThread.start();

        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(mockMqttService).connect();
        });

        application.shutdown();

        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(mockMqttService).close();
            verify(mockKafkaService).close();
            verify(mockInfluxService).close();
        });

        appThread.join(1000);
    }

    @Test
    void shouldHandleExceptionDuringStartAndStillShutdown() throws Exception {
        // Arrange
        doThrow(new RuntimeException("MQTT connect failure")).when(mockMqttService).connect();

        // Act
        Thread appThread = new Thread(() -> application.start());
        appThread.start();

        // Use Awaitility to wait until the verifications pass
        // This gives the application's shutdown hooks time to execute
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(mockMqttService).close();
            verify(mockKafkaService).close();
            verify(mockInfluxService).close();
        });

        // Optionally, wait for the thread to actually finish after verifications
        // This can help ensure the test runner doesn't prematurely kill the process
        // while the application is still cleaning up.
        appThread.join(1000); // Give it a bit more time to fully clean up if needed

        // If you want to ensure the thread *did* terminate due to the exception,
        // you could also add appThread.join() here and assert on appThread.isAlive()
        // being false, but Awaitility is more focused on verifying mock interactions.
    }
}
