package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful Flink job for testing stop/resume behavior with state persistence.
 *
 * Features:
 * - Keyed state (ValueState) tracking count per key
 * - Configurable checkpoint interval
 * - Configurable watermarking
 * - Configurable rate limiting
 * - State validation output
 */
public class StatefulJob {
    private static final Logger LOG = LoggerFactory.getLogger(StatefulJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Parse configuration from command-line arguments
        long checkpointInterval = 10000; // default 10 seconds
        boolean enableWatermarks = true;
        int ratePerSecond = 10;

        for (String arg : args) {
            if (arg.startsWith("--watermarks=")) {
                enableWatermarks = Boolean.parseBoolean(arg.split("=")[1]);
            } else if (arg.startsWith("--checkpoint-interval=")) {
                checkpointInterval = Long.parseLong(arg.split("=")[1]);
            } else if (arg.startsWith("--rate=")) {
                ratePerSecond = Integer.parseInt(arg.split("=")[1]);
            }
        }

        LOG.info("=== Stateful Job Configuration ===");
        LOG.info("Checkpoint interval: {} ms", checkpointInterval);
        LOG.info("Watermarks enabled: {}", enableWatermarks);
        LOG.info("Rate: {} records/second", ratePerSecond);
        LOG.info("==================================");

        // Enable checkpointing
        env.enableCheckpointing(checkpointInterval);

        // Generate keyed data (10 different keys)
        GeneratorFunction<Long, String> generatorFunction = index -> {
            String key = "key-" + (index % 10);
            long timestamp = System.currentTimeMillis();
            // Format: key|index|timestamp
            return key + "|" + index + "|" + timestamp;
        };

        DataGeneratorSource<String> source = new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(ratePerSecond),
                Types.STRING);

        WatermarkStrategy<String> watermarkStrategy = enableWatermarks
                ? WatermarkStrategy.<String>forMonotonousTimestamps()
                : WatermarkStrategy.noWatermarks();

        env.fromSource(source, watermarkStrategy, "Data Generator")
                .keyBy(value -> value.split("\\|")[0]) // key by first field
                .map(new StatefulCounter())
                .print();

        env.execute("Stateful Job");
    }

    /**
     * Stateful mapper that counts messages per key.
     * Maintains state across restarts when using savepoints/checkpoints.
     */
    public static class StatefulCounter extends RichMapFunction<String, String> {
        private static final Logger LOG = LoggerFactory.getLogger(StatefulCounter.class);
        private transient ValueState<Integer> countState;
        private transient ValueState<Long> firstSeenState;
        private transient ValueState<Long> lastSeenState;

        @Override
        public void open(OpenContext openContext) {
            ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>(
                    "count",
                    Types.INT
            );
            countState = getRuntimeContext().getState(countDescriptor);

            ValueStateDescriptor<Long> firstSeenDescriptor = new ValueStateDescriptor<>(
                    "first-seen",
                    Types.LONG
            );
            firstSeenState = getRuntimeContext().getState(firstSeenDescriptor);

            ValueStateDescriptor<Long> lastSeenDescriptor = new ValueStateDescriptor<>(
                    "last-seen",
                    Types.LONG
            );
            lastSeenState = getRuntimeContext().getState(lastSeenDescriptor);

            LOG.info("StatefulCounter opened for subtask {}",
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        }

        @Override
        public String map(String value) throws Exception {
            // Parse input: key|index|timestamp
            String[] parts = value.split("\\|");
            String key = parts[0];
            long index = Long.parseLong(parts[1]);
            long timestamp = Long.parseLong(parts[2]);

            // Update count
            Integer currentCount = countState.value();
            if (currentCount == null) {
                currentCount = 0;
                firstSeenState.update(timestamp);
                LOG.info("First message for key: {}", key);
            }
            currentCount += 1;
            countState.update(currentCount);
            lastSeenState.update(timestamp);

            // Every 100 messages, log state info for validation
            if (currentCount % 100 == 0) {
                Long firstSeen = firstSeenState.value();
                Long lastSeen = lastSeenState.value();
                long duration = lastSeen - firstSeen;
                LOG.info("State validation - Key: {}, Count: {}, Duration: {} ms",
                        key, currentCount, duration);
            }

            // Format output: key|count|index|processingTime
            return String.format("%s|count=%d|index=%d|processedAt=%d",
                    key, currentCount, index, System.currentTimeMillis());
        }

        @Override
        public void close() throws Exception {
            // Log final state for validation
            Integer finalCount = countState.value();
            if (finalCount != null) {
                LOG.info("StatefulCounter closing - Final count: {}", finalCount);
            }
        }
    }
}
