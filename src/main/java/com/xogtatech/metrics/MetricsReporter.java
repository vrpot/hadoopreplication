package com.xogtatech.metrics;

import com.xogtatech.hadoopreplication.config.ConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricsReporter {
    private static final Logger logger = LoggerFactory.getLogger(MetricsReporter.class);

    private final MetricsCollector metricsCollector;
    private final ScheduledExecutorService scheduler;
    private final int reportingIntervalSeconds;

    public MetricsReporter(MetricsCollector metricsCollector, ConfigurationManager configManager) {
        this.metricsCollector = metricsCollector;
        this.reportingIntervalSeconds = configManager.getMetricsReportingIntervalSeconds();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::reportMetrics, 0, reportingIntervalSeconds, TimeUnit.SECONDS);
    }

    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
    }

    private void reportMetrics() {
        logger.info("=== Hadoop Replication System Metrics ===");
        logger.info("Events Received: {}", metricsCollector.getEventsReceived());
        logger.info("Events Processed: {}", metricsCollector.getEventsProcessed());
        logger.info("Events Failed: {}", metricsCollector.getEventsFailed());
        logger.info("Bytes Replicated: {}", formatBytes(metricsCollector.getBytesReplicated()));
        
        logger.info("Event Type Counts:");
        for (Map.Entry<String, Long> entry : metricsCollector.getEventTypeCounts().entrySet()) {
            logger.info("  {}: {}", entry.getKey(), entry.getValue());
        }
        
        logger.info("===========================================");
        
        // Optionally reset metrics after reporting
        // metricsCollector.resetMetrics();
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp-1) + "";
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }
}
