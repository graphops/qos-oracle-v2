use std::env;
use std::time::Duration;

use rand::seq::SliceRandom;
use rand::Rng;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use tokio::time;
use uuid::Uuid;
use prost::Message;

// Use the exact schema from clickhouse/schema.proto
mod qos {
    include!(concat!(env!("OUT_DIR"), "/qos.rs"));
}
use qos::{ClientQueryProtobuf, IndexerQueryProtobuf};

#[tokio::main]
async fn main() {
    // Print librdkafka version
    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    // Read messages per second from the environment variable; default to 10 if not provided.
    let mps: u64 = env::var("MESSAGES_PER_SECOND")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .expect("MESSAGES_PER_SECOND must be a valid number");
    let delay = Duration::from_micros(1_000_000 / mps);

    // Allow configuring the broker address from environment
    let broker = env::var("KAFKA_BROKER")
        .unwrap_or_else(|_| "redpanda:9092".to_string());
    
    println!("Connecting to Kafka broker at {}", broker);
    
    // Wait for Redpanda to be fully ready
    println!("Waiting 15 seconds for Redpanda to initialize...");
    time::sleep(Duration::from_secs(15)).await;
    
    // Create a Kafka producer with extensive logging
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("message.timeout.ms", "30000")
        .set("security.protocol", "PLAINTEXT")
        .set("debug", "all")
        .set("enable.idempotence", "false")  // Simplify for testing
        .set("retries", "5")                 // Retry a few times
        .set("retry.backoff.ms", "1000")     // 1 second between retries
        .set("socket.timeout.ms", "10000")   // 10 seconds socket timeout
        .set("socket.keepalive.enable", "true")
        .create()
        .expect("Producer creation error");

    // Make sure this matches the topic name in ClickHouse Kafka engine
    let topic = "gateway_qos_topic";
    let mut counter = 0;

    loop {
        let message = generate_message_exact_schema();
        let payload = message.encode_to_vec();

        counter += 1;
        
        match producer
            .send(
                FutureRecord::to(topic)
                    .payload(&payload)
                    .key(&format!("{}", counter)),
                Duration::from_secs(0),
            )
            .await
        {
            Ok((partition, offset)) => println!("Delivered: ({}, {})", partition, offset),
            Err((e, _)) => eprintln!("Delivery error: {:?}", e),
        }

        // Sleep for the configured delay to maintain the desired messages-per-second rate.
        time::sleep(delay).await;
    }
}

// Function to generate a message with the exact schema from clickhouse/schema.proto
fn generate_message_exact_schema() -> ClientQueryProtobuf {
    let mut rng = rand::thread_rng();
    
    // Create random indexer queries
    let num_indexers = rng.gen_range(1..5);
    let mut indexer_queries = Vec::with_capacity(num_indexers);
    
    for _ in 0..num_indexers {
        let result = if rng.gen_bool(0.95) { "SUCCESS" } else { "ERROR" };
        
        let indexer_query = IndexerQueryProtobuf {
            indexer: Uuid::new_v4().to_string().into_bytes(),
            deployment: Uuid::new_v4().to_string().into_bytes(),
            allocation: Uuid::new_v4().to_string().into_bytes(),
            indexed_chain: format!("chain-{}", rng.gen_range(1..10)),
            url: format!("https://indexer-{}.example.com/graphql", rng.gen_range(1..100)),
            fee_grt: rng.gen_range(0.0001..0.01),
            response_time_ms: rng.gen_range(50..2000),
            seconds_behind: rng.gen_range(0..100),
            result: result.to_string(),
            indexer_errors: if result == "ERROR" { 
                "Internal server error".to_string() 
            } else { 
                "".to_string() 
            },
            blocks_behind: rng.gen_range(0..50),
        };
        
        indexer_queries.push(indexer_query);
    }
    
    // Create a ClientQueryProtobuf with EXACTLY the fields from schema.proto
    ClientQueryProtobuf {
        gateway_id: Uuid::new_v4().to_string(),
        receipt_signer: Uuid::new_v4().to_string().into_bytes(),
        query_id: Uuid::new_v4().to_string(),
        api_key: format!("api-key-{}", rng.gen_range(1..1000)),
        user_id: format!("user-{}", rng.gen_range(1..500)),
        subgraph: format!("subgraph-{}", rng.gen_range(1..20)),
        result: if rng.gen_bool(0.95) { "SUCCESS" } else { "ERROR" },
        response_time_ms: rng.gen_range(50..5000),
        request_bytes: rng.gen_range(100..5000),
        response_bytes: rng.gen_range(200..10000),
        total_fees_usd: rng.gen_range(0.001..0.1),
        indexer_queries,
    }
}
