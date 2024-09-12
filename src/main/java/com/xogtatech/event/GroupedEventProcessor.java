package com.xogtatech.event;

import com.xogtatech.hadoopreplication.config.ConfigurationManager;
import com.xogtatech.database.DatabaseManager;
import com.xogtatech.metrics.MetricsCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GroupedEventProcessor implements EventProcessor {
    private static final Logger logger = LoggerFactory.getLogger(GroupedEventProcessor.class);

    private final ConfigurationManager configManager;
    private final FileSystem sourceFs;
    private final FileSystem targetFs;
    private final DatabaseManager dbManager;
    private final MetricsCollector metricsCollector;

    public GroupedEventProcessor(ConfigurationManager configManager, 
                                 FileSystem sourceFs, 
                                 FileSystem targetFs, 
                                 DatabaseManager dbManager,
                                 MetricsCollector metricsCollector) {
        this.configManager = configManager;
        this.sourceFs = sourceFs;
        this.targetFs = targetFs;
        this.dbManager = dbManager;
        this.metricsCollector = metricsCollector;
    }

    @Override
    public void processEvents(List<Event> events) throws IOException {
        logger.info("Processing {} grouped events", events.size());
        String commonParentPath = findCommonParentPath(events);
        
        Path sourcePath = new Path(configManager.getSourceCluster(), commonParentPath);
        Path targetPath = new Path(configManager.getTargetCluster(), commonParentPath);

        try {
            DistCpOptions.Builder builder = new DistCpOptions.Builder(sourcePath, targetPath);
            builder.withSyncFolder(true)
                   .withOverwrite(false)
                   .withIgnoreFailures(false);

            DistCpOptions options = builder.build();

            Configuration conf = sourceFs.getConf();
            DistCp distCp = new DistCp(conf, options);
            distCp.execute();

            long bytesReplicated = calculateBytesReplicated(sourcePath);
            metricsCollector.addBytesReplicated(bytesReplicated);

            for (Event event : events) {
                dbManager.logEvent(event.getClass().getSimpleName(), 
                                   getEventPath(event), 
                                   targetPath.toString(), 
                                   getEventTimestamp(event), 
                                   "SUCCESS", 
                                   null);
                metricsCollector.incrementEventProcessed();
                metricsCollector.incrementEventTypeCount(event.getClass().getSimpleName());
            }
            logger.info("Successfully processed grouped events for path: {}", commonParentPath);
        } catch (Exception e) {
            logger.error("Error processing grouped events for path: " + commonParentPath, e);
            for (Event event : events) {
                dbManager.logEvent(event.getClass().getSimpleName(), 
                                   getEventPath(event), 
                                   targetPath.toString(), 
                                   getEventTimestamp(event), 
                                   "FAILED", 
                                   e.getMessage());
                metricsCollector.incrementEventFailed();
            }
            throw new IOException("Failed to process grouped events", e);
        }
    }



 

    private String findCommonParentPath(List<Event> events) {
        List<String> paths = new ArrayList<>();
        for (Event event : events) {
            paths.add(getEventPath(event));
        }
        String commonPrefix = paths.get(0);
        for (String path : paths) {
            while (!path.startsWith(commonPrefix)) {
                commonPrefix = commonPrefix.substring(0, commonPrefix.lastIndexOf('/'));
            }
        }
        return commonPrefix;
    }

    private String getEventPath(Event event) {
        if (event instanceof Event.CreateEvent) {
            return ((Event.CreateEvent) event).getPath();
        } else if (event instanceof Event.UnlinkEvent) {
            return ((Event.UnlinkEvent) event).getPath();
        } else if (event instanceof Event.AppendEvent) {
            return ((Event.AppendEvent) event).getPath();
        } else if (event instanceof Event.RenameEvent) {
            return ((Event.RenameEvent) event).getSrcPath();
        } else if (event instanceof Event.MetadataUpdateEvent) {
            return ((Event.MetadataUpdateEvent) event).getPath();
        } else if (event instanceof Event.TruncateEvent) {
            return ((Event.TruncateEvent) event).getPath();
        } else if (event instanceof Event.CloseEvent) {
            return ((Event.CloseEvent) event).getPath();
        }
        return "";
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

    private long calculateBytesReplicated(Path sourcePath) throws IOException {
        // This is a simplified calculation. In a real-world scenario, you might want to
        // implement a more sophisticated method to calculate the actual bytes transferred.
        return sourceFs.getContentSummary(sourcePath).getLength();
    }
}