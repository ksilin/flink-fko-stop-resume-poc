package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Test job for validating drain behavior with windowed aggregations.
 *
 * This job uses tumbling event-time windows to accumulate counts per key.
 * When stopped with drain, all windows should emit with MAX_WATERMARK.
 * When stopped without drain, window state should be preserved.
 *
 * Output format (structured for parsing):
 * - WINDOW_EMIT|key=X|window=[start-end]|count=N|sum=S|watermark=W|trigger=NORMAL|DRAIN
 * - EVENT_PROCESS|key=X|window=[start-end]|event_time=T|value=V
 *
 * Arguments:
 * --window-size=30      Window size in seconds (default: 30)
 * --watermarks=true     Enable watermarks (default: true)
 * --rate=2              Events per second per key (default: 2)
 * --keys=5              Number of distinct keys (default: 5)
 */
public class WindowedDrainTestJob {
    private static final Logger LOG = LoggerFactory.getLogger(WindowedDrainTestJob.class);

    // MAX_WATERMARK as defined in Flink's Watermark class
    public static final long MAX_WATERMARK = Long.MAX_VALUE - 1;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Parse arguments
        int windowSizeSeconds = 30;
        boolean enableWatermarks = true;
        int ratePerSecond = 2;  // Per key
        int numKeys = 5;
        long checkpointInterval = 10000;

        for (String arg : args) {
            if (arg.startsWith("--window-size=")) {
                windowSizeSeconds = Integer.parseInt(arg.split("=")[1]);
            } else if (arg.startsWith("--watermarks=")) {
                enableWatermarks = Boolean.parseBoolean(arg.split("=")[1]);
            } else if (arg.startsWith("--rate=")) {
                ratePerSecond = Integer.parseInt(arg.split("=")[1]);
            } else if (arg.startsWith("--keys=")) {
                numKeys = Integer.parseInt(arg.split("=")[1]);
            } else if (arg.startsWith("--checkpoint-interval=")) {
                checkpointInterval = Long.parseLong(arg.split("=")[1]);
            }
        }

        LOG.info("=== WindowedDrainTestJob Configuration ===");
        LOG.info("Window size: {} seconds", windowSizeSeconds);
        LOG.info("Watermarks enabled: {}", enableWatermarks);
        LOG.info("Rate: {} events/second/key", ratePerSecond);
        LOG.info("Number of keys: {}", numKeys);
        LOG.info("Checkpoint interval: {} ms", checkpointInterval);
        LOG.info("==========================================");

        // Enable checkpointing for state recovery
        env.enableCheckpointing(checkpointInterval);

        // Create source that generates keyed events with event time
        final int finalNumKeys = numKeys;
        GeneratorFunction<Long, Tuple3<String, Long, Integer>> generatorFunction = index -> {
            String key = "key-" + (index % finalNumKeys);
            // Event time advances with each event, but slowly to allow window accumulation
            // Each event is ~500ms apart in event time
            long eventTime = System.currentTimeMillis();
            int value = (int) (index % 1000);
            return Tuple3.of(key, eventTime, value);
        };

        DataGeneratorSource<Tuple3<String, Long, Integer>> source = new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(ratePerSecond * numKeys),
                Types.TUPLE(Types.STRING, Types.LONG, Types.INT)
        );

        // Watermark strategy
        WatermarkStrategy<Tuple3<String, Long, Integer>> watermarkStrategy;
        if (enableWatermarks) {
            watermarkStrategy = WatermarkStrategy
                    .<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, ts) -> event.f1);
        } else {
            watermarkStrategy = WatermarkStrategy
                    .<Tuple3<String, Long, Integer>>noWatermarks()
                    .withTimestampAssigner((event, ts) -> event.f1);
        }

        DataStream<Tuple3<String, Long, Integer>> stream = env
                .fromSource(source, watermarkStrategy, "Keyed Event Source");

        // Log each event as it's processed (sample every 10th to reduce noise)
        final int finalWindowSize = windowSizeSeconds;
        DataStream<Tuple3<String, Long, Integer>> logged = stream.map(event -> {
            // Only log every 10th event to reduce output volume
            if (event.f2 % 10 == 0) {
                long windowStart = (event.f1 / (finalWindowSize * 1000L)) * (finalWindowSize * 1000L);
                long windowEnd = windowStart + (finalWindowSize * 1000L);
                System.out.println(String.format(
                        "EVENT_PROCESS|key=%s|window=[%d-%d]|event_time=%d|value=%d|wall_time=%d",
                        event.f0, windowStart, windowEnd, event.f1, event.f2, System.currentTimeMillis()
                ));
            }
            return event;
        }).returns(Types.TUPLE(Types.STRING, Types.LONG, Types.INT));

        // Window aggregation with drain detection
        logged
                .keyBy(event -> event.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSizeSeconds)))
                .process(new DrainAwareWindowFunction())
                .name("drain-aware-window")
                .print();

        env.execute("WindowedDrainTestJob");
    }

    /**
     * Window function that detects drain-triggered emissions.
     *
     * When Flink drains a job, it emits MAX_WATERMARK which triggers all windows.
     * This function detects that condition and marks the output accordingly.
     */
    public static class DrainAwareWindowFunction
            extends ProcessWindowFunction<Tuple3<String, Long, Integer>, String, String, TimeWindow> {

        private static final Logger LOG = LoggerFactory.getLogger(DrainAwareWindowFunction.class);

        @Override
        public void process(String key, Context context,
                           Iterable<Tuple3<String, Long, Integer>> elements,
                           Collector<String> out) throws Exception {

            // Count elements and compute sum
            int count = 0;
            int sum = 0;
            long minEventTime = Long.MAX_VALUE;
            long maxEventTime = Long.MIN_VALUE;

            for (Tuple3<String, Long, Integer> element : elements) {
                count++;
                sum += element.f2;
                minEventTime = Math.min(minEventTime, element.f1);
                maxEventTime = Math.max(maxEventTime, element.f1);
            }

            TimeWindow window = context.window();
            long currentWatermark = context.currentWatermark();

            // Detect if this is a drain-triggered emission
            // MAX_WATERMARK = Long.MAX_VALUE - 1
            boolean isDrainTriggered = currentWatermark >= MAX_WATERMARK;
            String triggerType = isDrainTriggered ? "DRAIN" : "NORMAL";

            // Structured output for parsing
            String output = String.format(
                    "WINDOW_EMIT|key=%s|window=[%d-%d]|count=%d|sum=%d|" +
                    "min_event_time=%d|max_event_time=%d|watermark=%d|trigger=%s|wall_time=%d",
                    key,
                    window.getStart(),
                    window.getEnd(),
                    count,
                    sum,
                    minEventTime,
                    maxEventTime,
                    currentWatermark,
                    triggerType,
                    System.currentTimeMillis()
            );

            // Log with appropriate level
            if (isDrainTriggered) {
                LOG.warn("DRAIN DETECTED: {}", output);
            } else {
                LOG.info("Window emission: {}", output);
            }

            // Also print to stdout for easy log parsing
            System.out.println(output);

            out.collect(output);
        }
    }
}
