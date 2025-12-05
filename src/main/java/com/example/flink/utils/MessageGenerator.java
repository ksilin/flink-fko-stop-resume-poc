package com.example.flink.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Generates messages with various formats for testing different scenarios:
 * - Simple keyed messages
 * - Messages with timestamps
 * - Messages with unique IDs for tracking
 * - Bulk message generation for load testing
 */
public class MessageGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(MessageGenerator.class);
    private final Random random = new Random();

    /**
     * Generate a simple keyed message.
     * Format: key-{keyNum}|message-{index}|{timestamp}
     */
    public String generateKeyedMessage(int keyNum, long index) {
        long timestamp = System.currentTimeMillis();
        return String.format("key-%d|message-%d|%d", keyNum, index, timestamp);
    }

    /**
     * Generate messages for multiple keys.
     * Useful for testing stateful operations with different keys.
     */
    public List<String> generateKeyedMessages(int numKeys, int messagesPerKey) {
        List<String> messages = new ArrayList<>();
        for (int keyNum = 0; keyNum < numKeys; keyNum++) {
            for (int msgNum = 0; msgNum < messagesPerKey; msgNum++) {
                messages.add(generateKeyedMessage(keyNum, msgNum));
            }
        }
        LOG.info("Generated {} keyed messages ({} keys, {} messages/key)",
                messages.size(), numKeys, messagesPerKey);
        return messages;
    }

    /**
     * Generate a message with a unique ID for end-to-end tracking.
     * Format: {uuid}|key-{keyNum}|message-{index}|{timestamp}
     */
    public String generateTrackableMessage(int keyNum, long index) {
        String uuid = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis();
        return String.format("%s|key-%d|message-%d|%d", uuid, keyNum, index, timestamp);
    }

    /**
     * Generate bulk messages for load testing.
     */
    public List<String> generateBulkMessages(int count) {
        List<String> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            messages.add(generateSimpleMessage(i));
        }
        LOG.info("Generated {} bulk messages", count);
        return messages;
    }

    /**
     * Generate a simple sequential message.
     * Format: message-{index}|{timestamp}
     */
    public String generateSimpleMessage(long index) {
        long timestamp = System.currentTimeMillis();
        return String.format("message-%d|%d", index, timestamp);
    }

    /**
     * Generate messages with random delays (for timing tests).
     */
    public List<String> generateMessagesWithDelays(int count, int maxDelayMs) {
        List<String> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            messages.add(generateSimpleMessage(i));
            if (maxDelayMs > 0) {
                try {
                    Thread.sleep(random.nextInt(maxDelayMs));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        LOG.info("Generated {} messages with random delays (max {} ms)", count, maxDelayMs);
        return messages;
    }

    /**
     * Generate messages distributed across keys in round-robin fashion.
     */
    public List<String> generateRoundRobinMessages(int numKeys, int totalMessages) {
        List<String> messages = new ArrayList<>(totalMessages);
        for (int i = 0; i < totalMessages; i++) {
            int keyNum = i % numKeys;
            messages.add(generateKeyedMessage(keyNum, i / numKeys));
        }
        LOG.info("Generated {} messages distributed across {} keys", totalMessages, numKeys);
        return messages;
    }

    /**
     * Parse a message to extract the key.
     * Handles various message formats.
     */
    public static String extractKey(String message) {
        try {
            String[] parts = message.split("\\|");
            for (String part : parts) {
                if (part.startsWith("key-")) {
                    return part;
                }
            }
            // If no key found, use first part
            return parts[0];
        } catch (Exception e) {
            LOG.debug("Could not extract key from message: {}", message);
            return "default";
        }
    }

    /**
     * Parse a message to extract the index.
     */
    public static long extractIndex(String message) {
        try {
            String[] parts = message.split("\\|");
            for (String part : parts) {
                if (part.startsWith("message-")) {
                    return Long.parseLong(part.substring(8));
                }
            }
            return -1;
        } catch (Exception e) {
            LOG.debug("Could not extract index from message: {}", message);
            return -1;
        }
    }

    /**
     * Parse a message to extract the timestamp.
     */
    public static long extractTimestamp(String message) {
        try {
            String[] parts = message.split("\\|");
            // Timestamp is typically the last part
            return Long.parseLong(parts[parts.length - 1]);
        } catch (Exception e) {
            LOG.debug("Could not extract timestamp from message: {}", message);
            return -1;
        }
    }

    /**
     * Validate message format.
     */
    public static boolean isValidMessage(String message) {
        if (message == null || message.trim().isEmpty()) {
            return false;
        }
        return message.contains("|") && message.split("\\|").length >= 2;
    }
}
