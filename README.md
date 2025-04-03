# QoS Oracle V2

## Overview

QoS Oracle V2 is a data pipeline and reporting system for The Graph Network quality metrics. It collects, processes, and exposes quality-of-service data from Gateway nodes, providing valuable insights into indexer performance and network health.

## Architecture

The system consists of three main components:

1. **Data Ingestion** - Kafka/Redpanda message broker receives QoS data from Gateways
2. **Data Processing** - ClickHouse database ingests, stores, and aggregates the data into time-based views (5min, hourly, daily)
3. **Data Exposure** - GraphQL API serves these pre-aggregated metrics from Clickhouse views

## Features

- High-throughput data ingestion (via Kafka/Redpanda)
- Efficient storage with automatic TTL for raw data (managed by ClickHouse)
- Pre-aggregated metrics in 5-minute, hourly, and daily intervals
- Flexible GraphQL API for querying aggregated data with filtering, sorting, and time range selection
- Docker-based deployment for easy setup and operation using profiles

## Getting Started

### Prerequisites

- Docker and Docker Compose (v2 recommended)
- Git
- 4+ CPU cores recommended
- 16+ GB RAM recommended
- 250+ GB SSD storage recommended

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/graphops/qos-oracle.git # Or your repo URL
   cd qos-oracle
   ```

2. Start the services (Development profile includes API, DB, Kafka, and Test Producer):
   ```bash
   # Ensure you are in the root qos-oracle directory
   docker compose -f oracle/docker-compose.yml --profile dev up --build -d
   ```
   * Use `--profile deps` to start only ClickHouse and Redpanda

3. Verify the installation:
   ```bash
   # Check API health
   curl http://localhost:8000/health
   # Check running containers
   docker compose -f oracle/docker-compose.yml ps
   ```

4. Access the GraphQL endpoint at `http://localhost:8000/graphql`. You can use tools like Postman, Insomnia, `curl`, or potentially a browser-based playground if enabled.

### Configuration

The system can be configured through environment variables set in the `oracle/docker-compose.yml` file for the respective services:

- **`graphql-api` service:**
  * `CLICKHOUSE_DSN`: Full ClickHouse connection string (e.g., `tcp://graphql:graphql_password@clickhouse-server:9000/default?compression=lz4`). This replaces separate URL, DB, User, Pass variables.
  * `PORT`: Internal port the API listens on (default: `8000`).
  * `RUST_LOG`: Logging level (e.g., `info`, `debug`).
- **`test-producer` service:**
  * `KAFKA_BROKER`: Kafka/Redpanda broker address (e.g., `redpanda:9092`).
  * `MESSAGES_PER_SECOND`: Test producer message rate.

*(See `x-clickhouse-env` in `docker-compose.yml` for default user/password used in the default DSN)*

## Usage

### GraphQL API

The GraphQL API provides access to aggregated QoS metrics via three main queries: `deploymentAggregations`, `indexerAggregations`, and `allocationAggregations`.

**Endpoint:** `http://localhost:8000/graphql`

**Key Arguments:**

- `interval: AggregationInterval` (Optional: `FiveMinutes`, `Hourly`, `Daily`. Default: `Hourly`)
- `timeRange: TimeRangeInput` (Optional: Defaults to last 24h. Requires `from` or `to` if provided)
  * `from: String` (Optional: RFC3339 or Unix timestamp string, inclusive)
  * `to: String` (Optional: RFC3339 or Unix timestamp string, exclusive)
- `filter: AggregationFilterInput` (Optional: Filter by IDs)
  * `gatewayId: String`
  * `subgraph: String` (Deployment ID)
  * `indexer: String` (Hex Address)
  * `allocation: String` (Hex Address)
- `limit: Int` (Optional: Default 1000, Max 10000)
- `sort: [QuerySpecific]SortInput` (Optional: Sort by field and direction)

**Example Queries:**

1. **Get Hourly Deployment Aggregations (Default - Last 24 hours):**

   * **cURL:**
     ```bash
     curl -X POST http://localhost:8000/graphql \
          -H "Content-Type: application/json" \
          -d '{ "query": "{ deploymentAggregations { timeBucket subgraph gatewayId queryCount successProportion } }" }'
     ```
   * **GraphQL:**
     ```graphql
     {
       deploymentAggregations {
         timeBucket
         subgraph
         gatewayId
         queryCount
         successProportion
       }
     }
     ```

2. **Get Daily Indexer Aggregations for a Specific Indexer in October 2023:**

   * **cURL:**
     ```bash
     curl -X POST http://localhost:8000/graphql \
          -H "Content-Type: application/json" \
          -d '{
            "query": "query IndexerData($indexerId: String!) { indexerAggregations( interval: Daily, timeRange: { from: \"2023-10-01T00:00:00Z\", to: \"2023-11-01T00:00:00Z\" }, filter: { indexer: $indexerId } ) { timeBucket indexer gatewayId queryCount totalFeeGrt avgIndexerResponseTimeMs } }",
            "variables": { "indexerId": "0xYourIndexerAddressHere" }
          }'
     ```
   * **GraphQL (with variables):**
     ```graphql
     query IndexerData($indexerId: String!) {
       indexerAggregations(
         interval: Daily,
         timeRange: {
           from: "2023-10-01T00:00:00Z",
           to: "2023-11-01T00:00:00Z"
         },
         filter: { indexer: $indexerId }
       ) {
         timeBucket
         indexer
         gatewayId
         queryCount
         totalFeeGrt
         avgIndexerResponseTimeMs
       }
     }
     ```
     *Variables:*
     ```json
     {
       "indexerId": "0xYourIndexerAddressHere"
     }
     ```

3. **Get 5-Minute Allocation Aggregations Since a Specific Timestamp (Unix), Sorted by Query Count:**

   * **cURL:**
     ```bash
     curl -X POST http://localhost:8000/graphql \
          -H "Content-Type: application/json" \
          -d '{
            "query": "{ allocationAggregations( interval: FiveMinutes, timeRange: { from: \"1698300000\" }, sort: { field: QueryCount, direction: Desc }, limit: 50 ) { timeBucket subgraph indexer gatewayId queryCount avgSecondsBehind } }"
          }'
     ```
   * **GraphQL:**
     ```graphql
     {
       allocationAggregations(
         interval: FiveMinutes,
         timeRange: { from: "1698300000" },
         sort: { field: QueryCount, direction: Desc },
         limit: 50
       ) {
         timeBucket
         subgraph
         indexer
         gatewayId
         queryCount
         avgSecondsBehind
       }
     }
     ```

4. **Get Hourly Deployment Aggregations Up To a Specific Time for a Gateway:**

   * **cURL:**
     ```bash
     curl -X POST http://localhost:8000/graphql \
          -H "Content-Type: application/json" \
          -d '{
            "query": "query GatewayUntil($gwId: String!) { deploymentAggregations( timeRange: { to: \"2023-10-27T12:00:00Z\" }, filter: { gatewayId: $gwId } ) { timeBucket subgraph gatewayId queryCount avgResponseTimeMs } }",
            "variables": { "gwId": "gateway-mainnet-1" }
          }'
     ```
   * **GraphQL (with variables):**
     ```graphql
     query GatewayUntil($gwId: String!) {
       deploymentAggregations(
         timeRange: { to: "2023-10-27T12:00:00Z" },
         filter: { gatewayId: $gwId }
       ) {
         timeBucket
         subgraph
         gatewayId
         queryCount
         avgResponseTimeMs
       }
     }
     ```
     *Variables:*
     ```json
     {
       "gwId": "gateway-mainnet-1"
     }
     ```

## Development

### Project Structure

- `oracle/clickhouse/` - ClickHouse configuration and schema files
- `oracle/graphql-api/` - GraphQL API service (Rust)
- `oracle/test-producer/` - Test data generator for development (Rust)
- `oracle/docker-compose.yml` - Docker Compose configuration

### Running in Development Mode

```bash
# From the root qos-oracle directory
docker compose -f oracle/docker-compose.yml --profile dev up
```

*(Add `-d` to run detached)*

This will start all services including the test producer, which generates synthetic QoS data into the Redpanda topic consumed by ClickHouse.

### Monitoring

-   **GraphQL API Health:** `http://localhost:8000/health`
-   **GraphQL API Endpoint:** `http://localhost:8000/graphql`
-   **ClickHouse HTTP Interface:** `http://localhost:8123` (Credentials: `graphql`/`graphql_password` by default)
-   **Redpanda Kafka API:** `localhost:9092`
-   **Redpanda Prometheus Metrics:** `http://localhost:9644/metrics`
    *(Note: Redpanda Console is not included in the default compose file)*

## License

[Add license information here]

## Contributing

[Add contribution guidelines here]

## Acknowledgements

This project is maintained by [GraphOps](https://graphops.xyz) and is part of The Graph Network ecosystem.