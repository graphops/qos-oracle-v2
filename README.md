# QoS Oracle V2

## Overview

QoS Oracle V2 is a data pipeline and reporting system for The Graph Network quality metrics. It collects, processes, and exposes quality-of-service data from Gateway nodes, providing valuable insights into indexer performance and network health.

## Architecture

The system consists of three main components:

1. **Data Ingestion** - Kafka/Redpanda message broker receives QoS data from Gateways
2. **Data Processing** - ClickHouse database ingests, stores, and aggregates the data
3. **Data Exposure** - GraphQL API serves pre-aggregated metrics for analysis


## Features

- High-throughput data ingestion (2,000+ messages/second)
- Efficient storage with automatic TTL for raw data
- Pre-aggregated metrics in 5-minute, hourly, and daily intervals
- GraphQL API for flexible querying of historical data
- Docker-based deployment for easy setup and operation

## Getting Started

### Prerequisites

- Docker and Docker Compose
- 4+ CPU cores recommended
- 16+ GB RAM recommended
- 250+ GB SSD storage recommended

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/graphops/qos-oracle.git
   cd qos-oracle
   ```

2. Start the services:
   ```
   docker-compose -f oracle/docker-compose.yml up -d
   ```

3. Verify the installation:
   ```
   curl http://localhost:8000/health
   ```

4. Access the GraphQL playground at http://localhost:8000

### Configuration

The system can be configured through environment variables in the docker-compose.yml file:

- `CLICKHOUSE_URL` - ClickHouse server URL
- `CLICKHOUSE_DB` - ClickHouse database name
- `KAFKA_BROKER` - Kafka/Redpanda broker address
- `MESSAGES_PER_SECOND` - Test producer message rate (for development)

## Usage

### GraphQL API

The GraphQL API provides access to QoS reports and aggregated metrics. Here's an example query:

```graphql
query {
  qosReports(
    from: "2023-01-01T00:00:00Z", 
    to: "2023-01-02T00:00:00Z"
  ) {
    event_time
    gateway_id
    response_time_ms
    total_fees_usd
    indexer_queries {
      indexer
      deployment
      fee_grt
      response_time_ms
    }
  }
}
```

## Development

### Project Structure

- `oracle/clickhouse/` - ClickHouse configuration and schema files
- `oracle/graphql-api/` - GraphQL API service
- `oracle/test-producer/` - Test data generator for development
- `oracle/docker-compose.yml` - Docker Compose configuration

### Running in Development Mode

```
docker-compose -f oracle/docker-compose.yml --profile dev up
```

This will start all services including the test producer, which generates synthetic QoS data.

### Monitoring

- ClickHouse HTTP interface: http://localhost:8123
- Redpanda Console: http://localhost:8080
- GraphQL API: http://localhost:8000

## License

[Add license information here]

## Contributing

[Add contribution guidelines here]

## Acknowledgements

This project is maintained by [GraphOps](https://graphops.xyz) and is part of The Graph Network ecosystem.