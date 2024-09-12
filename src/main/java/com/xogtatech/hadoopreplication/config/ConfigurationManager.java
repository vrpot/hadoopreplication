package com.xogtatech.hadoopreplication.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationManager.class);
    private static final String CONFIG_FILE = "application.properties";

    private final Properties properties;

    public ConfigurationManager() {
        properties = new Properties();
        loadProperties();
    }

    private void loadProperties() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                throw new IOException("Unable to find " + CONFIG_FILE);
            }
            properties.load(input);
            logger.info("Configuration loaded successfully");
        } catch (IOException ex) {
            logger.error("Error loading configuration", ex);
            throw new RuntimeException("Failed to load configuration", ex);
        }
    }

    public String getSourceCluster() {
        return getProperty("source.cluster.uri");
    }

    public String getTargetCluster() {
        return getProperty("target.cluster.uri");
    }

    public String getKrb5ConfPath() {
        return getProperty("kerberos.krb5.conf.path");
    }

    public String getHdfsUserPrincipal() {
        return getProperty("kerberos.hdfs.user.principal");
    }

    public String getHdfsKeytabPath() {
        return getProperty("kerberos.hdfs.keytab.path");
    }

    public int getMaxConcurrentEvents() {
        return Integer.parseInt(getProperty("max.concurrent.events", "5"));
    }

    public int getProcessingIntervalMinutes() {
        return Integer.parseInt(getProperty("processing.interval.minutes", "10"));
    }

    public String getDatabaseUrl() {
        return getProperty("database.url");
    }

    public String getDatabaseUser() {
        return getProperty("database.user");
    }

    public String getDatabasePassword() {
        return getProperty("database.password");
    }

    public int getMetricsReportingIntervalSeconds() {
        return Integer.parseInt(getProperty("metrics.reporting.interval.seconds", "60"));
    }

    private String getProperty(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            throw new RuntimeException("Missing required configuration: " + key);
        }
        return value;
    }

    private String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}
