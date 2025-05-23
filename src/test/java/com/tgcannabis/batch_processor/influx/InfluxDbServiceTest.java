package com.tgcannabis.batch_processor.influx;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.write.Point;
import com.tgcannabis.batch_processor.config.BatchProcessorConfig;
import com.tgcannabis.batch_processor.model.SensorData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class InfluxDbServiceTest {
    private BatchProcessorConfig config;
    private InfluxDBClient mockClient;
    private WriteApi mockWriteApi;

    @BeforeEach
    void setUp() {
        config = mock(BatchProcessorConfig.class);

        when(config.getInfluxUrl()).thenReturn("http://localhost:8086");
        when(config.getInfluxOrg()).thenReturn("tg-cannabis");
        when(config.getInfluxBucket()).thenReturn("sensor-data");
        when(config.getInfluxToken()).thenReturn("new-token");

        mockClient = mock(InfluxDBClient.class);
        mockWriteApi = mock(WriteApi.class);
    }

    private SensorData validSensorData() {
        SensorData data = new SensorData();
        data.setSensorId("sensor-001");
        data.setSensorType("temperature");
        data.setLocation("greenhouse-1");
        data.setValue(25.5);
        data.setTimestamp(System.currentTimeMillis());
        return data;
    }

    @Test
    void shouldThrowIfSensorDataIsNull() {
        InfluxDbService service = new InfluxDbService(config, mockClient, mockWriteApi);
        assertThrows(NullPointerException.class, () -> service.writeSensorData(null, "topic"));
    }

    @Test
    void shouldRetryIfClientNotInitialized() {
        SensorData data = validSensorData();

        // Spy allows us to partially mock InfluxDbService
        InfluxDbService service = spy(new InfluxDbService(config, null, null));

        // Mock behavior of initializeClient() to simulate successful reinit
        doAnswer(invocation -> {
            // Manually set the mock client and write API after "reinit"
            service.setInfluxDBClient(mockClient);
            service.setWriteApi(mockWriteApi);
            return null;
        }).when(service).initializeClient();

        // Should not throw due to retry and successful reinit
        assertDoesNotThrow(() -> service.writeSensorData(data, "retry-topic"));

        // Confirm that reinitialization was attempted
        verify(service).initializeClient();
        verify(mockWriteApi).writePoint(any(Point.class));
    }


    @Test
    void shouldSkipIfSensorDataIncomplete() {
        SensorData invalidData = new SensorData();
        invalidData.setSensorId(null);
        invalidData.setSensorType(null);
        invalidData.setValue(10.5);
        invalidData.setTimestamp(System.currentTimeMillis());

        InfluxDbService service = new InfluxDbService(config, mockClient, mockWriteApi);

        assertDoesNotThrow(() -> service.writeSensorData(invalidData, "topic"));
        verify(mockWriteApi, never()).writePoint(any(Point.class));
    }

    @Test
    void shouldWriteValidSensorData() {
        InfluxDbService service = new InfluxDbService(config, mockClient, mockWriteApi);
        SensorData validData = validSensorData();

        assertDoesNotThrow(() -> service.writeSensorData(validData, "topic-1"));
        verify(mockWriteApi, times(1)).writePoint(any(Point.class));
    }

    @Test
    void shouldCloseClientGracefully() {
        InfluxDbService service = new InfluxDbService(config, mockClient, mockWriteApi);
        service.close();
        verify(mockClient).close();
    }

    @Test
    void shouldNotFailIfCloseCalledTwice() {
        InfluxDbService service = new InfluxDbService(config, mockClient, mockWriteApi);

        service.close();
        service.close(); // should not crash or call close again
        verify(mockClient, times(1)).close();
    }
}
