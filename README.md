# Testing Strategies for Kafka Streams: From Unit Tests to Integration

## Overview
This repository demonstrate unit and integration tests for two Kafka Streams topologies: [UserTopology](./src/main/java/space/zeinab/demo/streamsTest/service/UserTopology.java) and [UserActivityTopology](./src/main/java/space/zeinab/demo/streamsTest/service/UserActivityTopology.java) . Topologies cover key concepts such as stateless and stateful operations, windowing, and the use of state stores

Unit tests for the topologies are implemented using TopologyTestDriver.

## Prerequisites
* Java 17 or higher
* Maven
* Docker (optional, for running Docker Compose which include Zookeeper and Apache Kafka)


## Running the Application
1. **Clone the repository**
   ```sh
   git clone <repository-url>
   cd java-kafka-streams-test-demo
   ```

2. **Run Unit Tests**
   ```sh
   mvn clean verify
   ```
   