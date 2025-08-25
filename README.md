# Ecommerce Kafka Microservices Project

This project is a Java-based microservices system for ecommerce alura.com.br course "Kafka: Fast delegate, evolução e cluster de brokers" using Apache Kafka for messaging. The project is organized as a Maven multi-module, with each service in its own module.

## Project Structure

- `ecommerce/` (parent Maven project)
  - `common-kafka/` (shared Kafka utilities)
  - `service-new-order/` (handles new orders)
  - `service-email/` (sends email notifications)
  - `service-fraud-detector/` (fraud detection)
  - `service-log/` (event logging)
  - `service-http-ecommerce/` (HTTP API for ecommerce integration)
  - `service-users/` (user management and local database)
  

## Prerequisites

- Java 21
- Maven
- [Apache Kafka](https://kafka.apache.org/downloads) (recommended: Scala 2.13 version)

## Running Kafka (Windows)

1. **Download Kafka**
   - Go to [Kafka Downloads](https://kafka.apache.org/downloads)
   - Download the Scala 2.13 version
   - Extract the archive

2. **Start Zookeeper**
   - Open Git Bash (do not use CMD or PowerShell)
   - Navigate to the Kafka folder
   - Run:
     ```sh
     bin/zookeeper-server-start.sh config/zookeeper.properties
     ```

3. **Start Kafka Broker**
   - In another Git Bash terminal, run:
     ```sh
     bin/kafka-server-start.sh config/server.properties
     ```

> **Note:** On Windows, always use Git Bash to run `.sh` scripts.

## Building the Project

From the `ecommerce` directory, run:

```sh
mvn clean install
```

## Running the Services

Each service can be run individually. Enter the desired service folder and run:

```sh
mvn exec:java
```

Or run the generated JAR in `target/`:

```sh
java -jar target/<jar-name>.jar
```

### Available Services

- **service-new-order**: receives and publishes new orders to Kafka
- **service-email**: consumes events and sends emails
- **service-fraud-detector**: detects possible fraud in orders
- **service-log**: logs events
- **service-http-ecommerce**: exposes HTTP endpoints for external integration
- **service-users**: manages users and local persistence (e.g., `users_database.db`)

## Notes

- All services require Kafka to be running.
- The `common-kafka` module contains shared utilities for all services.
- The `service-users` module uses a local database file (`.db` in `target/`).

## License

Educational project based on the [Alura course](https://www.alura.com.br/).
