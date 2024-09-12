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
import java.util.List;

public class UngroupedEventProcessor implements EventProcessor {
    private static final Logger logger = LoggerFactory.getLogger(UngroupedEventProcessor.class);

    private final ConfigurationManager configManager;
    private final FileSystem sourceFs;
    private final FileSystem targetFs;
    private final DatabaseManager dbManager;
    private final MetricsCollector metricsCollector;

    public UngroupedEventProcessor(ConfigurationManager configManager,
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
        if (events.size() != 1) {
            throw new IllegalArgumentException("UngroupedEventProcessor should process only one event at a time");
        }
        Event event = events.get(0);
        String eventType = event.getClass().getSimpleName();
        String sourcePath = getEventPath(event);
        String targetPath = sourcePath.replace(configManager.getSourceCluster(), configManager.getTargetCluster());

        try {
            processEvent(event, sourcePath, targetPath);
            dbManager.logEvent(eventType, sourcePath, targetPath, getEventTimestamp(event), "SUCCESS", null);
            metricsCollector.incrementEventProcessed();
            metricsCollector.incrementEventTypeCount(eventType);
            logger.info("Successfully processed event: {} - {}", eventType, sourcePath);
        } catch (Exception e) {
            logger.error("Error processing event: " + eventType + " - " + sourcePath, e);
            dbManager.logEvent(eventType, sourcePath, targetPath, getEventTimestamp(event), "FAILED", e.getMessage());
            metricsCollector.incrementEventFailed();
            throw new IOException("Failed to process ungrouped event", e);
        }
    }

    private void processEvent(Event event, String sourcePath, String targetPath) throws IOException {
        if (event instanceof Event.CreateEvent || event instanceof Event.AppendEvent || event instanceof Event.CloseEvent) {
            handleFileModificationEvent(sourcePath, targetPath);
        } else if (event instanceof Event.UnlinkEvent) {
            handleDeleteEvent(targetPath);
        } else if (event instanceof Event.RenameEvent) {
            Event.RenameEvent renameEvent = (Event.RenameEvent) event;
            handleRenameEvent(renameEvent.getSrcPath(), renameEvent.getDstPath());
        } else if (event instanceof Event.MetadataUpdateEvent) {
            handleMetadataUpdateEvent(targetPath, (Event.MetadataUpdateEvent) event);
        } else if (event instanceof Event.TruncateEvent) {
            handleTruncateEvent(sourcePath, targetPath);
        } else {
            logger.warn("Unhandled event type: " + event.getClass().getSimpleName());
        }
    }

    private void handleFileModificationEvent(String sourcePath, String targetPath) throws IOException {
        Path src = new Path(sourcePath);
        Path dst = new Path(targetPath);

        DistCpOptions.Builder builder = new DistCpOptions.Builder(src, dst);
        builder.withSyncFolder(true)
               .withOverwrite(true)
               .withIgnoreFailures(false);

        DistCpOptions options = builder.build();

        Configuration conf = sourceFs.getConf();
        try {
            DistCp distCp = new DistCp(conf, options);
            distCp.execute();

            long bytesReplicated = sourceFs.getFileStatus(src).getLen();
            metricsCollector.addBytesReplicated(bytesReplicated);
        } catch (Exception e) {
            logger.error("Error during DistCp operation", e);
            throw new IOException("Failed to execute DistCp", e);
        }
    }

    private void handleDeleteEvent(String targetPath) throws IOException {
        Path path = new Path(targetPath);
        if (targetFs.exists(path)) {
            boolean deleted = targetFs.delete(path, true);
            if (deleted) {
                logger.info("Successfully deleted: {}", targetPath);
            } else {
                logger.warn("Failed to delete: {}", targetPath);
                throw new IOException("Failed to delete path: " + targetPath);
            }
        }
    }

    private void handleRenameEvent(String srcPath, String dstPath) throws IOException {
        Path sourcePath = new Path(srcPath.replace(configManager.getSourceCluster(), configManager.getTargetCluster()));
        Path destPath = new Path(dstPath.replace(configManager.getSourceCluster(), configManager.getTargetCluster()));
        if (targetFs.exists(sourcePath)) {
            boolean renamed = targetFs.rename(sourcePath, destPath);
            if (renamed) {
                logger.info("Successfully renamed: {} to {}", srcPath, dstPath);
            } else {
                logger.warn("Failed to rename: {} to {}", srcPath, dstPath);
                throw new IOException("Failed to rename path: " + srcPath + " to " + dstPath);
            }
        } else {
            logger.warn("Source path does not exist for rename operation: {}", sourcePath);
            throw new IOException("Source path does not exist for rename operation: " + sourcePath);
        }
    }

    private void handleMetadataUpdateEvent(String targetPath, Event.MetadataUpdateEvent event) throws IOException {
        Path path = new Path(targetPath);
        if (targetFs.exists(path)) {
            try {
                if (event.getMetadataType() == Event.MetadataUpdateEvent.MetadataType.TIMES) {
                    targetFs.setTimes(path, event.getMtime(), event.getAtime());
                } else if (event.getMetadataType() == Event.MetadataUpdateEvent.MetadataType.OWNER) {
                    targetFs.setOwner(path, event.getOwnerName(), event.getGroupName());
                } else if (event.getMetadataType() == Event.MetadataUpdateEvent.MetadataType.PERMS) {
                    targetFs.setPermission(path, new org.apache.hadoop.fs.permission.FsPermission(event.getPerms()));
                } else if (event.getMetadataType() == Event.MetadataUpdateEvent.MetadataType.REPLICATION) {
                    short newReplication = (short) event.getReplication(); // Cast int to short
                    boolean success = targetFs.setReplication(path, newReplication);
                    if (!success) {
                        logger.warn("Failed to set replication for path: {}. Possibly a directory or unsupported operation.", targetPath);
                    }
                }
                logger.info("Updated metadata for: {}", targetPath);
            } catch (IOException e) {
                logger.error("Failed to update metadata for: " + targetPath, e);
                throw new IOException("Failed to update metadata for path: " + targetPath, e);
            }
        } else {
            logger.warn("Target path does not exist for metadata update: {}", targetPath);
            throw new IOException("Target path does not exist for metadata update: " + targetPath);
        }
    }

    private void handleTruncateEvent(String sourcePath, String targetPath) throws IOException {
        Path src = new Path(sourcePath);
        Path dst = new Path(targetPath);
    
        // For truncate, we need to copy the entire file again to ensure consistency
        handleFileModificationEvent(sourcePath, targetPath);
    
        // After copying, we need to truncate the target file to match the source file's length
        long newLength = sourceFs.getFileStatus(src).getLen();
        if (targetFs.truncate(dst, newLength)) {
            logger.info("Handled truncate event for: {} (new length: {})", targetPath, newLength);
        } else {
            logger.warn("Failed to truncate file: {} to length: {}", targetPath, newLength);
            throw new IOException("Failed to truncate file: " + targetPath);
        }
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
}