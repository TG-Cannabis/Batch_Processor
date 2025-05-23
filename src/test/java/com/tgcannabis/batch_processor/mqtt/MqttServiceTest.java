package com.tgcannabis.batch_processor.mqtt;

import com.tgcannabis.batch_processor.config.BatchProcessorConfig;
import org.eclipse.paho.client.mqttv3.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MqttServiceTest {
    private MqttClient mockClient;
    private MqttService mqttService;

    @BeforeEach
    void setup() throws Exception {
        BatchProcessorConfig config = mock(BatchProcessorConfig.class);
        when(config.getMqttBroker()).thenReturn("tcp://localhost:1883");
        when(config.getMqttClientId()).thenReturn("test-client");
        when(config.getMqttTopicFilter()).thenReturn("sensors/#");

        mockClient = mock(MqttClient.class);
        mqttService = new MqttService(config, mockClient);
    }

    @Test
    void shouldThrowIfNoMessageHandlerSet() {
        assertThrows(NullPointerException.class, () -> mqttService.connect());
    }

    @Test
    void shouldConnectAndSubscribeSuccessfully() throws Exception {
        when(mockClient.isConnected()).thenReturn(true);

        mqttService.setMessageHandler((topic, msg) -> {
        });
        mqttService.connect();

        verify(mockClient).connect(any(MqttConnectOptions.class));
        verify(mockClient).setCallback(any(MqttCallback.class));
    }

    @Test
    void shouldLogAndRethrowOnConnectionFailure() throws Exception {
        mqttService.setMessageHandler((topic, msg) -> {
        });
        doThrow(new MqttException(0)).when(mockClient).connect(any());

        assertThrows(MqttException.class, () -> mqttService.connect());

        verify(mockClient).connect(any());
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
    void shouldCloseGracefullyWhenConnected() throws Exception {
        when(mockClient.isConnected()).thenReturn(true);

        mqttService.close();

        verify(mockClient).disconnect();
        verify(mockClient).close();
    }

    @Test
    void shouldHandleCloseQuietlyWhenNotConnected() throws Exception {
        when(mockClient.isConnected()).thenReturn(false);

        mqttService.close();

        verify(mockClient).close();
        verify(mockClient, never()).disconnect();
    }

    @Test
    void shouldNotSubscribeIfClientNotConnected() throws Exception {
        when(mockClient.isConnected()).thenReturn(false);
        mqttService.setMessageHandler((topic, msg) -> {
        });
        mqttService.connect();

        // Trigger reconnect callback manually
        MqttCallbackExtended callback = captureCallback();
        callback.connectComplete(false, "tcp://localhost:1883");

        verify(mockClient, never()).subscribe(anyString(), anyInt());
    }

    @Test
    void shouldLogErrorWhenSubscribeFails() throws Exception {
        when(mockClient.isConnected()).thenReturn(true);
        mqttService.setMessageHandler((topic, msg) -> {
        });
        mqttService.connect();

        // Trigger callback
        MqttCallbackExtended callback = captureCallback();

        doThrow(new MqttException(1)).when(mockClient).subscribe(anyString(), anyInt());

        // Now simulate connection complete
        callback.connectComplete(false, "tcp://localhost:1883");

        verify(mockClient).subscribe("sensors/#", 1);
    }

    @Test
    void shouldProcessMessageWhenHandlerSet() throws Exception {
        BiConsumer<String, String> handler = mock(BiConsumer.class);
        mqttService.setMessageHandler(handler);

        mqttService.connect(); // Triggers callback registration
        MqttCallbackExtended callback = captureCallback();

        MqttMessage msg = new MqttMessage("test-payload".getBytes());
        callback.messageArrived("test/topic", msg);

        verify(handler).accept("test/topic", "test-payload");
    }

    private MqttCallbackExtended captureCallback() throws Exception {
        verify(mockClient).setCallback(any());
        var argCaptor = org.mockito.ArgumentCaptor.forClass(MqttCallback.class);
        verify(mockClient).setCallback(argCaptor.capture());
        return (MqttCallbackExtended) argCaptor.getValue();
    }

    @Test
    void shouldCatchExceptionFromHandler() throws Exception {
        BiConsumer<String, String> handler = (t, m) -> {
            throw new RuntimeException("Boom");
        };
        mqttService.setMessageHandler(handler);
        mqttService.connect();
        MqttCallbackExtended callback = captureCallback();

        MqttMessage msg = new MqttMessage("boom".getBytes());

        assertDoesNotThrow(() -> callback.messageArrived("some/topic", msg));
    }
}
