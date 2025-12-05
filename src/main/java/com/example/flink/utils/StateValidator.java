package com.example.flink.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for validating state consistency across job restarts.
 *
 * This class helps track expected vs actual state values during testing,
 * particularly useful for verifying that savepoint/checkpoint recovery
 * maintains state correctness.
 */
public class StateValidator {
    private static final Logger LOG = LoggerFactory.getLogger(StateValidator.class);

    private final Map<String, Integer> expectedCounts = new HashMap<>();
    private final Map<String, Integer> actualCounts = new HashMap<>();
    private final Map<String, Long> lastSeenTimestamps = new HashMap<>();

    /**
     * Record an expected count for a key.
     * Used to track what the state should be before a restart.
     */
    public void recordExpectedCount(String key, int count) {
        expectedCounts.put(key, count);
        LOG.debug("Recorded expected count for key {}: {}", key, count);
    }

    /**
     * Record an actual count for a key.
     * Used after a restart to verify state recovery.
     */
    public void recordActualCount(String key, int count) {
        actualCounts.put(key, count);
        lastSeenTimestamps.put(key, System.currentTimeMillis());
        LOG.debug("Recorded actual count for key {}: {}", key, count);
    }

    /**
     * Validate that actual counts match or exceed expected counts.
     * In a properly functioning stateful job, counts should continue from
     * where they left off.
     *
     * @return true if validation passes, false otherwise
     */
    public boolean validateCounts() {
        boolean valid = true;

        for (Map.Entry<String, Integer> entry : expectedCounts.entrySet()) {
            String key = entry.getKey();
            int expected = entry.getValue();
            Integer actual = actualCounts.get(key);

            if (actual == null) {
                LOG.error("Validation FAILED: No actual count found for key {}", key);
                valid = false;
            } else if (actual < expected) {
                LOG.error("Validation FAILED: Key {} - Expected >= {}, Got {}",
                        key, expected, actual);
                valid = false;
            } else {
                LOG.info("Validation PASSED: Key {} - Expected >= {}, Got {}",
                        key, expected, actual);
            }
        }

        return valid;
    }

    /**
     * Validate that counts are increasing (indicating the job is processing).
     * Call this method twice with a delay in between to verify progress.
     */
    public boolean validateProgress(String key, int minIncrease) {
        Integer count1 = actualCounts.get(key);
        if (count1 == null) {
            LOG.warn("No count found for key {} in first measurement", key);
            return false;
        }

        // Wait for some processing
        try {
            Thread.sleep(5000); // 5 seconds
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        // Check again
        Integer count2 = actualCounts.get(key);
        if (count2 == null) {
            LOG.warn("No count found for key {} in second measurement", key);
            return false;
        }

        int increase = count2 - count1;
        if (increase >= minIncrease) {
            LOG.info("Progress validation PASSED: Key {} increased by {} (>= {})",
                    key, increase, minIncrease);
            return true;
        } else {
            LOG.error("Progress validation FAILED: Key {} increased by {} (< {})",
                    key, increase, minIncrease);
            return false;
        }
    }

    /**
     * Reset all tracked state.
     */
    public void reset() {
        expectedCounts.clear();
        actualCounts.clear();
        lastSeenTimestamps.clear();
        LOG.info("State validator reset");
    }

    /**
     * Get summary of current state.
     */
    public String getSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== State Validation Summary ===\n");
        sb.append(String.format("Expected counts: %d keys\n", expectedCounts.size()));
        sb.append(String.format("Actual counts: %d keys\n", actualCounts.size()));

        for (String key : expectedCounts.keySet()) {
            Integer expected = expectedCounts.get(key);
            Integer actual = actualCounts.get(key);
            String status = (actual != null && actual >= expected) ? "✓" : "✗";
            sb.append(String.format("  %s Key %s: Expected >= %d, Actual %s\n",
                    status, key, expected, actual != null ? actual : "N/A"));
        }

        return sb.toString();
    }

    /**
     * Parse a Flink job output line and extract state information.
     * Expected format: "key|count=N|..."
     */
    public void parseAndRecordOutput(String output) {
        try {
            String[] parts = output.split("\\|");
            if (parts.length >= 2) {
                String key = parts[0].trim();
                for (String part : parts) {
                    if (part.startsWith("count=")) {
                        int count = Integer.parseInt(part.substring(6));
                        recordActualCount(key, count);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            LOG.debug("Could not parse output line: {}", output);
        }
    }
}
