package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateless Flink job for testing stop/resume behavior without state persistence.
 *
 * Features:
 * - No state (stateless processing)
 * - Configurable watermarking
 * - Configurable rate limiting
 * - Message tracking for validation
 */
public class StatelessJob {
    private static final Logger LOG = LoggerFactory.getLogger(StatelessJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Parse configuration from command-line arguments
        boolean enableWatermarks = true;
        int ratePerSecond = 10;

        for (String arg : args) {
            if (arg.startsWith("--watermarks=")) {
                enableWatermarks = Boolean.parseBoolean(arg.split("=")[1]);
            } else if (arg.startsWith("--rate=")) {
                ratePerSecond = Integer.parseInt(arg.split("=")[1]);
            }
        }

        LOG.info("=== Stateless Job Configuration ===");
        LOG.info("Watermarks enabled: {}", enableWatermarks);
        LOG.info("Rate: {} records/second", ratePerSecond);
        LOG.info("===================================");

        // Generate sequential messages with metadata
        GeneratorFunction<Long, String> generatorFunction = index -> {
            long timestamp = System.currentTimeMillis();
            // Format: message-{index}|{timestamp}
            return "message-" + index + "|" + timestamp;
        };

        DataGeneratorSource<String> source = new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(ratePerSecond),
                Types.STRING
        );

        WatermarkStrategy<String> watermarkStrategy = enableWatermarks
                ? WatermarkStrategy.<String>forMonotonousTimestamps()
                : WatermarkStrategy.noWatermarks();

        env.fromSource(source, watermarkStrategy, "Data Generator")
                .map(value -> {
                    String[] parts = value.split("\\|");
                    String message = parts[0];
                    long generatedTime = Long.parseLong(parts[1]);
                    long processingTime = System.currentTimeMillis();
                    long latency = processingTime - generatedTime;

                    // Extract index for logging
                    String indexStr = message.substring(message.lastIndexOf('-') + 1);
                    long index = Long.parseLong(indexStr);

                    // Log every 100 messages for validation
                    if (index % 100 == 0) {
                        LOG.info("Processing validation - Index: {}, Latency: {} ms",
                                index, latency);
                    }

                    // Format output: message|latency|processedAt
                    return String.format("Processed: %s|latency=%dms|processedAt=%d",
                            message, latency, processingTime);
                })
                .print();

        env.execute("Stateless Job");
    }
}
