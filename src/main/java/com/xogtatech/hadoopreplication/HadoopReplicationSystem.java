package com.xogtatech.hadoopreplication;

import com.xogtatech.hadoopreplication.config.ConfigurationManager;
import com.xogtatech.event.EventProcessor;
import com.xogtatech.event.GroupedEventProcessor;
import com.xogtatech.event.UngroupedEventProcessor;
import com.xogtatech.metrics.MetricsCollector;
import com.xogtatech.metrics.MetricsReporter;
import com.xogtatech.database.DatabaseManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;

public class HadoopReplicationSystem {
    private static final Logger logger = LoggerFactory.getLogger(HadoopReplicationSystem.class);

    private final ConfigurationManager configManager;
    private final Configuration hadoopConf;
    private final FileSystem sourceFs;
    private final FileSystem targetFs;
    private final DatabaseManager dbManager;
    private final ScheduledExecutorService scheduler;
    private final Map<String, Event> eventBuffer;
    private final ExecutorService eventProcessorExecutor;
    private final EventProcessor groupedEventProcessor;
    private final EventProcessor ungroupedEventProcessor;
    private final MetricsCollector metricsCollector;
    private final MetricsReporter metricsReporter;

    public HadoopReplicationSystem() throws IOException {
        this.configManager = new ConfigurationManager();
        this.hadoopConf = setupHadoopConfiguration();
        this.sourceFs = FileSystem.get(URI.create(configManager.getSourceCluster()), hadoopConf);
        this.targetFs = FileSystem.get(URI.create(configManager.getTargetCluster()), hadoopConf);
        this.dbManager = new DatabaseManager(configManager);
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.eventBuffer = new ConcurrentHashMap<>();
        this.eventProcessorExecutor = Executors.newFixedThreadPool(configManager.getMaxConcurrentEvents());
        this.metricsCollector = new MetricsCollector();
        this.metricsReporter = new MetricsReporter(metricsCollector, configManager);
        this.groupedEventProcessor = new GroupedEventProcessor(configManager, sourceFs, targetFs, dbManager, metricsCollector);
        this.ungroupedEventProcessor = new UngroupedEventProcessor(configManager, sourceFs, targetFs, dbManager, metricsCollector);
    }

    private Configuration setupHadoopConfiguration() throws IOException {
        Configuration conf = new Configuration();
        System.setProperty("java.security.krb5.conf", configManager.getKrb5ConfPath());
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.security.authorization", "true");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(configManager.getHdfsUserPrincipal(), configManager.getHdfsKeytabPath());
        return conf;
    }

    public void start() throws IOException {
        logger.info("Starting Hadoop Replication System");
        metricsReporter.start();
        
        HdfsAdmin admin = new HdfsAdmin(URI.create(configManager.getSourceCluster()), hadoopConf);
        DFSInotifyEventInputStream eventStream = admin.getInotifyEventStream();

        scheduler.scheduleAtFixedRate(this::processEvents, 0, configManager.getProcessingIntervalMinutes(), TimeUnit.MINUTES);

        try {
            while (true) {
                EventBatch batch = eventStream.take();
                for (Event event : batch.getEvents()) {
                    String path = getEventPath(event);
                    eventBuffer.put(path, event);
                    metricsCollector.incrementEventReceived();
                }
            }
        } catch (Exception e) {
            logger.error("Error in event processing loop", e);
            shutdown();
        }
    }

    private void processEvents() {
        logger.info("Processing buffered events");
        List<Event> events = new ArrayList<>(eventBuffer.values());
        eventBuffer.clear();

        events.sort((e1, e2) -> Long.compare(getEventTimestamp(e1), getEventTimestamp(e2)));

        Map<String, List<Event>> groupedEvents = groupEventsByPath(events);
        List<Event> ungroupedEvents = new ArrayList<>();

        for (Map.Entry<String, List<Event>> entry : groupedEvents.entrySet()) {
            if (entry.getValue().size() == 1) {
                ungroupedEvents.add(entry.getValue().get(0));
            } else {
                processGroupedEvents(entry.getValue());
            }
        }

        processUngroupedEvents(ungroupedEvents);
    }

    private long getEventTimestamp(Event event) {
        if (event instanceof Event.CreateEvent) {
            return ((Event.CreateEvent) event).getCtime();
        } else if (event instanceof Event.UnlinkEvent) {
            return System.currentTimeMillis(); // UnlinkEvent doesn't have a timestamp, use current time
        } else if (event instanceof Event.AppendEvent) {
            return System.currentTimeMillis(); // AppendEvent doesn't have a timestamp, use current time
        } else if (event instanceof Event.RenameEvent) {
            return ((Event.RenameEvent) event).getTimestamp();
        } else if (event instanceof Event.MetadataUpdateEvent) {
            return ((Event.MetadataUpdateEvent) event).getMetadataType() == Event.MetadataUpdateEvent.MetadataType.TIMES ?
                   ((Event.MetadataUpdateEvent) event).getMtime() : System.currentTimeMillis();
        } else if (event instanceof Event.TruncateEvent) {
            return System.currentTimeMillis(); // TruncateEvent doesn't have a timestamp, use current time
        } else if (event instanceof Event.CloseEvent) {
            return ((Event.CloseEvent) event).getTimestamp();
        }
        return System.currentTimeMillis(); // Default to current time for unknown event types
    }

    private Map<String, List<Event>> groupEventsByPath(List<Event> events) {
        Map<String, List<Event>> groupedEvents = new HashMap<>();
        for (Event event : events) {
            String path = getEventPath(event);
            String parentPath = new Path(path).getParent().toString();
            groupedEvents.computeIfAbsent(parentPath, k -> new ArrayList<>()).add(event);
        }
        return groupedEvents;
    }

    private void processGroupedEvents(List<Event> events) {
        try {
            groupedEventProcessor.processEvents(events);
        } catch (Exception e) {
            logger.error("Error processing grouped events", e);
            metricsCollector.incrementEventFailed();
        }
    }

    private void processUngroupedEvents(List<Event> events) {
        CompletableFuture<?>[] futures = events.stream()
            .map(event -> CompletableFuture.runAsync(() -> {
                try {
                    ungroupedEventProcessor.processEvents(Collections.singletonList(event));
                } catch (Exception e) {
                    logger.error("Error processing ungrouped event: " + getEventPath(event), e);
                    metricsCollector.incrementEventFailed();
                }
            }, eventProcessorExecutor))
            .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
    }

    private String getEventPath(Event event) {
        if (event instanceof Event.CreateEvent) {
            return ((Event.CreateEvent) event).getPath();
        } else if (event instanceof Event.UnlinkEvent) {
            return ((Event.UnlinkEvent) event).getPath();
        } else if (event instanceof Event.AppendEvent) {
            return ((Event.AppendEvent) event).getPath();
        } else if (event instanceof Event.RenameEvent) {
            Event.RenameEvent renameEvent = (Event.RenameEvent) event;
            return renameEvent.getSrcPath() + " -> " + renameEvent.getDstPath();
        } else if (event instanceof Event.MetadataUpdateEvent) {
            return ((Event.MetadataUpdateEvent) event).getPath();
        } else if (event instanceof Event.TruncateEvent) {
            return ((Event.TruncateEvent) event).getPath();
        } else if (event instanceof Event.CloseEvent) {
            return ((Event.CloseEvent) event).getPath();
        }
        
        logger.warn("Unhandled event type: " + event.getClass().getSimpleName());
        return "Unknown path for event type: " + event.getClass().getSimpleName();
    }

    public void shutdown() {
        logger.info("Shutting down Hadoop Replication System");
        scheduler.shutdown();
        eventProcessorExecutor.shutdown();
        metricsReporter.stop();
        try {
            sourceFs.close();
            targetFs.close();
            dbManager.close();
        } catch (IOException e) {
            logger.error("Error closing resources during shutdown", e);
        }
    }

    public static void main(String[] args) {
        try {
            HadoopReplicationSystem system = new HadoopReplicationSystem();
            system.start();
        } catch (Exception e) {
            logger.error("Error starting Hadoop Replication System", e);
        }
    }
}