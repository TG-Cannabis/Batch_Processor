package com.tgcannabis.batch_processor.mqtt;

import com.tgcannabis.batch_processor.config.BatchProcessorConfig;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MqttServiceTest {
    private BatchProcessorConfig config;
    private MqttClient mockClient;
    private MqttService mqttService;

    @BeforeEach
    void setup() throws Exception {
        config = mock(BatchProcessorConfig.class);
        when(config.getMqttBroker()).thenReturn("tcp://localhost:1883");
        when(config.getMqttClientId()).thenReturn("test-client");
        when(config.getMqttTopicFilter()).thenReturn("sensors/#");

        mockClient = mock(MqttClient.class);
        when(mockClient.isConnected()).thenReturn(true);

        mqttService = new MqttService(config, mockClient);
    }

    @Test
    void shouldThrowIfNoMessageHandlerSet() {
        assertThrows(NullPointerException.class, () -> mqttService.connect());
    }

    @Test
    void shouldSubscribeWhenConnected() throws Exception {
        BiConsumer<String, String> handler = (topic, message) -> {
        };
        mqttService.setMessageHandler(handler);
        mqttService.connect();

        verify(mockClient).connect(any(MqttConnectOptions.class));
    }

    @Test
    void shouldCloseGracefully() throws Exception {
        when(mockClient.isConnected()).thenReturn(true);

        mqttService.close();

        verify(mockClient).disconnect();
        verify(mockClient).close();
    }
}
