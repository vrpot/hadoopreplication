# Hadoop Replication System

## Overview

The Hadoop Replication System is a robust solution designed to replicate data(HDFS) between Hadoop clusters in near real-time. It utilizes HDFS inotify events to detect changes in the source cluster and efficiently replicates these changes to the target cluster, ensuring data consistency and minimizing latency.

## Features

- Near real-time replication of HDFS data
- Support for various HDFS events (create, modify, delete, rename, etc.)
- Kerberos authentication for secure cluster access
- Configurable batch processing of events
- Metrics collection for monitoring replication performance
- Logging and error handling for reliable operation
- Support for both grouped and ungrouped event processing

## Prerequisites

- Java 11 or later
- Apache Maven 3.6.0 or later
- Access to source and target Hadoop clusters (CDP 7.1.9)
- MySQL database for event tracking
- Kerberos configuration (if using secure clusters)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/vrpot/hadoop-replication-system.git
   cd hadoop-replication-system
   ```

2. Update the `application.properties` file in `src/main/resources` with your specific configuration:
   ```
   # Hadoop Cluster Configuration
   source.cluster.uri=hdfs://source-cluster:8020
   target.cluster.uri=hdfs://target-cluster:8020

   # Kerberos Configuration
   kerberos.krb5.conf.path=/etc/krb5.conf
   kerberos.hdfs.user.principal=hdfs_user@REALM.COM
   kerberos.hdfs.keytab.path=/path/to/hdfs_user.keytab

   # Database Configuration
   database.url=jdbc:mysql://localhost:3306/replication_db
   database.user=replication_user
   database.password=your_secure_password

   # Other configurations...
   ```

3. Build the project:
   ```
   mvn clean install
   ```

## Usage

To run the Hadoop Replication System:

1. Ensure that your Kerberos tickets are up-to-date (if using secure clusters).

2. Execute the JAR file:
   ```
   java -jar target/hadoop-replication-system-1.0-SNAPSHOT.jar
   ```

3. Monitor the application logs for replication status and any errors.

## Configuration

The system can be configured through the `application.properties` file. Key configuration options include:

- `max.concurrent.events`: Maximum number of events to process concurrently
- `processing.interval.minutes`: Interval for batch processing of events
- `metrics.reporting.interval.seconds`: Interval for reporting metrics

Refer to the `application.properties` file for a complete list of configuration options.

## Logging

Logging is configured in the `log4j2.xml` file located in `src/main/resources`. By default, logs are written to both the console and a rolling file appender.

## Monitoring

The system collects various metrics, including:

- Number of events processed
- Number of events failed
- Amount of data replicated

These metrics are reported at regular intervals as specified in the configuration.

## Troubleshooting

- Check the application logs for detailed error messages and stack traces.
- Ensure that the Hadoop clusters are accessible and that the provided Kerberos credentials are valid.
- Verify that the MySQL database is properly configured and accessible.

## Contributing

Contributions to the Hadoop Replication System are welcome. Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

Apache-2.0

## Contact

For any queries or support, please contact contact@xogtatech.com.
