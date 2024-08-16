import { Kafka, KafkaConfig, Producer } from 'kafkajs';
import crypto from 'crypto';

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
    "status_code": number,
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

const kafkaConfig: KafkaConfig = { brokers: ['localhost:9092'] };
const kafka = new Kafka(kafkaConfig);
const queryTopic = "gateway_client_query_results";
const indexerTopic = "gateway_indexer_attempts";
let topics = [queryTopic, indexerTopic];

const hexAddresses: string[] = Array(10).fill(0).map(() => '0x' + crypto.randomBytes(20).toString('hex'));
const deployments: string[] = Array(10).fill(0).map(() => 'Qm' + crypto.randomBytes(22).toString('base64').replace(/[+/]/g, '').slice(0, 44));

function getRandomHexAddress(): string {
  return hexAddresses[Math.floor(Math.random() * hexAddresses.length)];
}

function getRandomDeployment(): string {
  return deployments[Math.floor(Math.random() * deployments.length)];
}
function generateRandomUrl(): string {
    const domain = crypto.randomBytes(10).toString('hex') + '.com';
    return `http://${domain}`;
}

function getRandomInt(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function getRandomTopic(): string {
    return topics[Math.floor(Math.random() * topics.length)];
}

let baseValueClient: ClientQueryResults = {
    "gateway_id": "Gateway ID",
    "query_id": "",
    "ray_id": "Ray ID",
    "network_chain": "mainnet",
    "graph_env": "mainnet",
    "timestamp": 0,
    "api_key": "API KEY",
    "user": "USER",
    "deployment": "",
    "network": "arbitrum-one",
    "indexed_chain": "arbitrum-one",
    "response_time_ms": 100,
    "budget": "Budget",
    "query_count": 1,
    "fee": 1,
    "fee_usd": 1,
    "status": "200 OK",
    "status_code": 200,
};

let baseValueIndexer: IndexerQueryFormat = {
    "gateway_id": "Gateway ID",
    "query_id": "",
    "ray_id": "Ray ID",
    "network_chain": "mainnet",
    "graph_env": "mainnet",
    "timestamp": 0,
    "api_key": "API KEY",
    "user_address": "USER",
    "deployment": "",
    "network": "arbitrum-one",
    "indexed_chain": "arbitrum-one",
    "response_time_ms": 100,
    "fee": 1,
    "status": "200 OK",
    "status_code": 200,
    "indexer": "Indexer address",
    "url": "http....",
    "seconds_behind": 1,
    "blocks_behind": 1,
    "allocation": "Some allocation id here",
    "indexer_errors": "no errors here bruv",
    "legacy_scalar": false,
    "utility": 0
};

let baseValues = {
    "gateway_client_query_results": baseValueClient,
    "gateway_indexer_attempts": baseValueIndexer
};

const producer = kafka.producer();

async function main() {
    await producer.connect();
    let i = 0;

    while(true) {
        let topic = getRandomTopic();
        let value = JSON.parse(JSON.stringify(baseValues[topic])); // Deep copy

        // Common randomizations for both topics
        value.gateway_id = crypto.randomUUID();
        value.query_id = "Query ID " + i.toString();
        value.ray_id = crypto.randomUUID();
        value.timestamp = Date.now();
        value.api_key = crypto.randomBytes(16).toString('hex');
        value.deployment = getRandomDeployment();
        value.response_time_ms = getRandomInt(50, 300);
        value.fee = getRandomInt(20, 100);

        if (topic === queryTopic) {
            (value as ClientQueryResults).user = getRandomHexAddress();
            (value as ClientQueryResults).budget = (Math.random() * 1000).toFixed(2);
            (value as ClientQueryResults).query_count = getRandomInt(1, 10);
            (value as ClientQueryResults).fee_usd = getRandomInt(1, 10);
        } else {
            (value as IndexerQueryFormat).user_address = getRandomHexAddress();
            (value as IndexerQueryFormat).indexer = getRandomHexAddress();
            (value as IndexerQueryFormat).url = generateRandomUrl();
            (value as IndexerQueryFormat).seconds_behind = getRandomInt(1, 60);
            (value as IndexerQueryFormat).blocks_behind = getRandomInt(1, 500);
            (value as IndexerQueryFormat).allocation = crypto.randomUUID();
            (value as IndexerQueryFormat).legacy_scalar = Math.random() < 0.5;
            (value as IndexerQueryFormat).utility = Math.random();
        }

        let messageSent = await producer.send({
            topic: topic,
            messages: [
                { headers: { source: 'graph-gateway' }, value: JSON.stringify(value) },
            ],
        });

        console.log(`Message sent to ${topic}:`, messageSent);
        i++;
        await new Promise(f => setTimeout(f, 50));
    }
}

main().catch(console.error);