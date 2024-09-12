package com.xogtatech.database;

import com.xogtatech.hadoopreplication.config.ConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.ArrayList;

public class DatabaseManager {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseManager.class);

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;

    public DatabaseManager(ConfigurationManager configManager) {
        this.dbUrl = configManager.getDatabaseUrl();
        this.dbUser = configManager.getDatabaseUser();
        this.dbPassword = configManager.getDatabasePassword();
        initializeDatabase();
    }

    private void initializeDatabase() {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE IF NOT EXISTS replication_events (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY," +
                    "event_type VARCHAR(50) NOT NULL," +
                    "source_path VARCHAR(255) NOT NULL," +
                    "target_path VARCHAR(255) NOT NULL," +
                    "timestamp BIGINT NOT NULL," +
                    "status VARCHAR(20) NOT NULL," +
                    "error_message TEXT)";
            stmt.execute(sql);
            logger.info("Database initialized successfully");
        } catch (SQLException e) {
            logger.error("Error initializing database", e);
            throw new RuntimeException("Failed to initialize database", e);
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }

    public void logEvent(String eventType, String sourcePath, String targetPath, long timestamp, String status, String errorMessage) {
        String sql = "INSERT INTO replication_events (event_type, source_path, target_path, timestamp, status, error_message) VALUES (?, ?, ?, ?, ?, ?)";
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, eventType);
            pstmt.setString(2, sourcePath);
            pstmt.setString(3, targetPath);
            pstmt.setLong(4, timestamp);
            pstmt.setString(5, status);
            pstmt.setString(6, errorMessage);
            pstmt.executeUpdate();
            logger.debug("Event logged: {} - {} - {}", eventType, sourcePath, status);
        } catch (SQLException e) {
            logger.error("Error logging event", e);
        }
    }

    public List<ReplicationEvent> getFailedEvents(long sinceTimestamp) {
        List<ReplicationEvent> failedEvents = new ArrayList<>();
        String sql = "SELECT * FROM replication_events WHERE status = 'FAILED' AND timestamp > ? ORDER BY timestamp DESC";
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, sinceTimestamp);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    failedEvents.add(new ReplicationEvent(
                            rs.getString("event_type"),
                            rs.getString("source_path"),
                            rs.getString("target_path"),
                            rs.getLong("timestamp"),
                            rs.getString("status"),
                            rs.getString("error_message")
                    ));
                }
            }
        } catch (SQLException e) {
            logger.error("Error retrieving failed events", e);
        }
        return failedEvents;
    }

    public void updateEventStatus(long eventId, String newStatus, String errorMessage) {
        String sql = "UPDATE replication_events SET status = ?, error_message = ? WHERE id = ?";
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, newStatus);
            pstmt.setString(2, errorMessage);
            pstmt.setLong(3, eventId);
            pstmt.executeUpdate();
            logger.debug("Event status updated: {} - {}", eventId, newStatus);
        } catch (SQLException e) {
            logger.error("Error updating event status", e);
        }
    }

    public void cleanupOldEvents(long retentionPeriod) {
        long cutoffTimestamp = System.currentTimeMillis() - retentionPeriod;
        String sql = "DELETE FROM replication_events WHERE timestamp < ?";
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, cutoffTimestamp);
            int deletedRows = pstmt.executeUpdate();
            logger.info("Cleaned up {} old events", deletedRows);
        } catch (SQLException e) {
            logger.error("Error cleaning up old events", e);
        }
    }

    public void close() {
        // No need to close anything here as we're using try-with-resources
        logger.info("DatabaseManager closed");
    }

    public static class ReplicationEvent {
        private final String eventType;
        private final String sourcePath;
        private final String targetPath;
        private final long timestamp;
        private final String status;
        private final String errorMessage;

        public ReplicationEvent(String eventType, String sourcePath, String targetPath, long timestamp, String status, String errorMessage) {
            this.eventType = eventType;
            this.sourcePath = sourcePath;
            this.targetPath = targetPath;
            this.timestamp = timestamp;
            this.status = status;
            this.errorMessage = errorMessage;
        }

        // Getters for all fields
        public String getEventType() { return eventType; }
        public String getSourcePath() { return sourcePath; }
        public String getTargetPath() { return targetPath; }
        public long getTimestamp() { return timestamp; }
        public String getStatus() { return status; }
        public String getErrorMessage() { return errorMessage; }
    }
}
