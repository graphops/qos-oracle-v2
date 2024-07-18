import { Kafka, KafkaConfig, Producer } from 'kafkajs';

interface IndexerQueryFormat {
    "gateway_id": string,
    "query_id": string,
    "ray_id": string,
    "network_chain": string,
    "graph_env": string,
    "timestamp": number,
    "api_key": string,
    "user_address": string,
    "deployment": string,
    "network": string,
    "indexed_chain": string,
    "indexer": string,
    "url": string,
    "fee": number,
    "legacy_scalar": boolean,
    "utility": number,
    "seconds_behind": number,
    "blocks_behind": number,
    "response_time_ms": number,
    "allocation": string,
    "indexer_errors": string,
    "status": string,
    "status_code": string,
}

interface ClientQueryResults {
  "gateway_id": string,
  "query_id": string,
  "ray_id": string,
  "network_chain": string,
  "graph_env": string,
  "timestamp": number,
  "api_key": string,
  "user": string,
  "deployment": string,
  "network": string,
  "indexed_chain": string,
  "response_time_ms": number,
  "budget": string,
  "query_count": number,
  "fee": number,
  "fee_usd": number,
  "status": string,
  "status_code": number,
}

const kafkaConfig: KafkaConfig = { brokers: ['localhost:9092'] }
const kafka = new Kafka(kafkaConfig)

const queryTopic = "gateway_client_query_results"
const indexerTopic = "gateway_indexer_attempts"

const value = {
  "gateway_id": "Gateway ID",
  "query_id": "Query ID",
  "ray_id": "Ray ID",
  "network_chain": "mainnet",
  "graph_env": "mainnet",
  "timestamp": 1,
  "api_key": "API KEY",
  "user": "USER",
  "deployment": "Qm...",
  "network": "arbitrum-one",
  "indexed_chain": "arbitrum-one",
  "response_time_ms": 100,
  "budget": "Budget",
  "query_count": 1,
  "fee": 1,
  "fee_usd": 1,
  "status": "200 OK",
  "status_code": 200,
}

const producer = kafka.producer()
await producer.connect()
let messageSent = await producer.send({
  topic: queryTopic,
  messages: [
    { headers: { source: 'graph-gateway' }, value: JSON.stringify(value) },
  ],
})
console.log(messageSent)