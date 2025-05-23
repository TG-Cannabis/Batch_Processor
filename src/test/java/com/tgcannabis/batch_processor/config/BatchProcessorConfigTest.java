package com.tgcannabis.batch_processor.config;

import io.github.cdimascio.dotenv.Dotenv;
import io.github.cdimascio.dotenv.DotenvBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class BatchProcessorConfigTest {

    @Test
    void shouldLoadAllConfigsFromDotenvFallback() {
        Dotenv mockDotenv = mock(Dotenv.class);
        DotenvBuilder mockBuilder = mock(DotenvBuilder.class);

        try (MockedStatic<Dotenv> dotenvStatic = mockStatic(Dotenv.class)) {
            dotenvStatic.when(Dotenv::configure).thenReturn(mockBuilder);
            when(mockBuilder.ignoreIfMissing()).thenReturn(mockBuilder);
            when(mockBuilder.load()).thenReturn(mockDotenv);

            when(mockDotenv.get("INFLUX_TOKEN")).thenReturn("test-token");
            when(mockDotenv.get("INFLUX_ORG")).thenReturn("test-org");
            when(mockDotenv.get("INFLUX_BUCKET")).thenReturn("test-bucket");

            BatchProcessorConfig config = new BatchProcessorConfig();

            assertEquals("test-token", config.getInfluxToken());
            assertEquals("test-org", config.getInfluxOrg());
            assertEquals("test-bucket", config.getInfluxBucket());
        }
    }

    @Test
    void shouldThrowIfRequiredInfluxTokenMissing() {
        Dotenv mockDotenv = mock(Dotenv.class);
        DotenvBuilder mockBuilder = mock(DotenvBuilder.class);
        try (MockedStatic<Dotenv> dotenvStatic = mockStatic(Dotenv.class)) {
            dotenvStatic.when(Dotenv::configure).thenReturn(mockBuilder);
            when(mockBuilder.ignoreIfMissing()).thenReturn(mockBuilder);
            when(mockBuilder.load()).thenReturn(mockDotenv);

            when(mockDotenv.get("INFLUX_TOKEN")).thenReturn(null);
            when(mockDotenv.get("INFLUX_ORG")).thenReturn("org");
            when(mockDotenv.get("INFLUX_BUCKET")).thenReturn("bucket");

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, BatchProcessorConfig::new);
            assertEquals("InfluxDB write token is required.", ex.getMessage());
        }
    }

    @Test
    void shouldThrowIfRequiredInfluxOrgMissing() {
        Dotenv mockDotenv = mock(Dotenv.class);
        DotenvBuilder mockBuilder = mock(DotenvBuilder.class);

        try (MockedStatic<Dotenv> dotenvStatic = mockStatic(Dotenv.class)) {
            dotenvStatic.when(Dotenv::configure).thenReturn(mockBuilder);
            when(mockBuilder.ignoreIfMissing()).thenReturn(mockBuilder);
            when(mockBuilder.load()).thenReturn(mockDotenv);

            when(mockDotenv.get("INFLUX_TOKEN")).thenReturn("token");
            when(mockDotenv.get("INFLUX_ORG")).thenReturn(null);
            when(mockDotenv.get("INFLUX_BUCKET")).thenReturn("bucket");

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, BatchProcessorConfig::new);
            assertEquals("InfluxDB organization is required.", ex.getMessage());
        }
    }

    @Test
    void shouldThrowIfRequiredInfluxBucketMissing() {
        Dotenv mockDotenv = mock(Dotenv.class);
        DotenvBuilder mockBuilder = mock(DotenvBuilder.class);

        try (MockedStatic<Dotenv> dotenvStatic = mockStatic(Dotenv.class)) {
            dotenvStatic.when(Dotenv::configure).thenReturn(mockBuilder);
            when(mockBuilder.ignoreIfMissing()).thenReturn(mockBuilder);
            when(mockBuilder.load()).thenReturn(mockDotenv);

            when(mockDotenv.get("INFLUX_TOKEN")).thenReturn("token");
            when(mockDotenv.get("INFLUX_ORG")).thenReturn("org");
            when(mockDotenv.get("INFLUX_BUCKET")).thenReturn(null);

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, BatchProcessorConfig::new);
            assertEquals("InfluxDB bucket name is required.", ex.getMessage());
        }
    }

}
