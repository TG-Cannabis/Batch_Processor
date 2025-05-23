package com.tgcannabis.batch_processor.processing;

import com.google.gson.Gson;
import com.tgcannabis.batch_processor.influx.InfluxDbService;
import com.tgcannabis.batch_processor.kafka.KafkaService;
import com.tgcannabis.batch_processor.model.SensorData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class SensorDataHandlerTest {

    private KafkaService kafkaService;
    private InfluxDbService influxDbService;
    private SensorDataHandler handler;

    private final Gson gson = new Gson();

    @BeforeEach
    void setUp() {
        kafkaService = mock(KafkaService.class);
        influxDbService = mock(InfluxDbService.class);
        handler = new SensorDataHandler(kafkaService, influxDbService);
    }

    @Test
    void shouldProcessValidSensorData() {
        SensorData data = new SensorData(
                "temperature",
                "growlab",
                "sensor_1",
                24.5,
                System.currentTimeMillis());
        String json = gson.toJson(data);

        handler.accept("sensors/temperature", json);

        verify(kafkaService, times(1)).sendMessage(eq("sensor_1"), eq(json));
        verify(influxDbService, times(1)).writeSensorData(eq(data), eq("sensors/temperature"));
    }

    @Test
    void shouldIgnoreMalformedJson() {
        String malformedJson = "{not a json}";

        handler.accept("sensors/invalid", malformedJson);

        verifyNoInteractions(kafkaService);
        verifyNoInteractions(influxDbService);
    }

    @Test
    void shouldIgnoreNullSensorId() {
        String json = gson.toJson(new SensorData(
                "temperature",
                "growlab",
                null,
                24.5,
                System.currentTimeMillis()));

        handler.accept("sensors/humidity", json);

        verifyNoInteractions(kafkaService);
        verifyNoInteractions(influxDbService);
    }

    @Test
    void shouldIgnoreEmptyObjects() {
        String json = gson.toJson(new SensorData());

        handler.accept("sensors/temperature", json);

        verifyNoInteractions(kafkaService);
        verifyNoInteractions(influxDbService);
    }
}
