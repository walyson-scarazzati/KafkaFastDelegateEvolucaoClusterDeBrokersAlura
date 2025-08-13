# Ecommerce Kafka Microservices Project

This project is from [alura.com.br](https://www.alura.com.br/)   Kafka: Fast delegate, evolução e cluster de brokers is a set of Java microservices for an ecommerce system, using Apache Kafka for messaging. It is organized as a Maven multi-module project, with each service in its own module.

## Project Structure

- `ecommerce/` (parent Maven project)
  - `common-kafka/` (shared Kafka utilities)
  - `service-new-order/` (handles new orders)
  - `service-email/` (handles email notifications)
  - `service-fraud-detector/` (detects fraud)
  - `service-log/` (logs events)

## Prerequisites

- Java 21
- Maven
- [Apache Kafka](https://kafka.apache.org/downloads) (Scala 2.13 version recommended)

## Running Kafka (Windows)

1. **Download Kafka**
   - Go to [Kafka Downloads](https://kafka.apache.org/downloads)
   - Download the Scala 2.13 version
   - Extract the archive

2. **Start Zookeeper**
   - Open Git Bash (not CMD or PowerShell)
   - Navigate to the Kafka folder
   - Run:
     ```sh
     bin/zookeeper-server-start.sh config/zookeeper.properties
     ```

3. **Start Kafka Broker**
   - In another Git Bash window, run:
     ```sh
     bin/kafka-server-start.sh config/server.properties
     ```

> **Note:** On Windows, use Git Bash. CMD and PowerShell may not work with the provided shell scripts.

## Building the Project

From the `ecommerce` directory, run:

```sh
mvn clean install
```

## Running the Services

Each service can be run from its own directory using:

```sh
mvn exec:java
```

Or by running the generated JAR files in the `target/` directories.

## License

This project is for educational purposes.
