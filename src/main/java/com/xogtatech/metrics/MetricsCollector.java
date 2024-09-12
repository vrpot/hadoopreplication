package com.xogtatech.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class MetricsCollector {
    private final AtomicLong eventsReceived = new AtomicLong(0);
    private final AtomicLong eventsProcessed = new AtomicLong(0);
    private final AtomicLong eventsFailed = new AtomicLong(0);
    private final AtomicLong bytesReplicated = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> eventTypeCount = new ConcurrentHashMap<>();

    public void incrementEventReceived() {
        eventsReceived.incrementAndGet();
    }

    public void incrementEventProcessed() {
        eventsProcessed.incrementAndGet();
    }

    public void incrementEventFailed() {
        eventsFailed.incrementAndGet();
    }

    public void addBytesReplicated(long bytes) {
        bytesReplicated.addAndGet(bytes);
    }

    public void incrementEventTypeCount(String eventType) {
        eventTypeCount.computeIfAbsent(eventType, k -> new AtomicLong(0)).incrementAndGet();
    }

    public long getEventsReceived() {
        return eventsReceived.get();
    }

    public long getEventsProcessed() {
        return eventsProcessed.get();
    }

    public long getEventsFailed() {
        return eventsFailed.get();
    }

    public long getBytesReplicated() {
        return bytesReplicated.get();
    }

    public Map<String, Long> getEventTypeCounts() {
        Map<String, Long> counts = new ConcurrentHashMap<>();
        for (Map.Entry<String, AtomicLong> entry : eventTypeCount.entrySet()) {
            counts.put(entry.getKey(), entry.getValue().get());
        }
        return counts;
    }

    public void resetMetrics() {
        eventsReceived.set(0);
        eventsProcessed.set(0);
        eventsFailed.set(0);
        bytesReplicated.set(0);
        eventTypeCount.clear();
    }
}
