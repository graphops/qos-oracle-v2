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
use chrono;

// Assume the generated Protobuf code is available in the `qos` module.
// The build script should generate the Protobuf code from your schema.proto.
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
        .set("statistics.interval.ms", "30000") // Get stats every 30 seconds
        .set("api.version.request", "true")
        .set("api.version.request.timeout.ms", "5000")
        .create()
        .expect("Failed to create Kafka producer");

    // Verify topic exists before sending
    println!("Sending test messages to topic 'gateway_qos_topic'");
    
    // Message counter
    let mut count = 0;
    
    loop {
        let msg = generate_random_message();
        let mut buf = Vec::new();
        msg.encode(&mut buf).expect("Failed to encode message");

        let query_id = String::from_utf8_lossy(&msg.query_id).to_string();
        
        // Produce message to the target Kafka topic.
        let record = FutureRecord::to("gateway_qos_topic")
            .payload(&buf)
            .key(&query_id);
            
        match producer.send(record, Duration::from_secs(10)).await {
            Ok(delivery) => {
                count += 1;
                if count % 10 == 0 {  // Only log every 10 messages to reduce noise
                    println!("Successfully delivered message #{}: {:?}", count, delivery);
                }
            },
            Err((e, _)) => eprintln!("Delivery error: {:?}", e),
        }

        // Sleep for the configured delay to maintain the desired messages-per-second rate.
        time::sleep(delay).await;
    }
}

fn generate_random_message() -> ClientQueryProtobuf {
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
    
    // Create a random gateway query with the indexer queries
    ClientQueryProtobuf {
        query_id: Uuid::new_v4().to_string().into_bytes(),
        gateway_id: Uuid::new_v4().to_string().into_bytes(),
        event_time: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        indexer_queries,
        subgraph: format!("subgraph-{}", rng.gen_range(1..20)),
    }
}

fn generate_random_indexer_query() -> IndexerQueryProtobuf {
    let mut rng = rand::thread_rng();
    
    // Sample data
    let indexed_chains = ["ethereum", "polygon", "arbitrum", "optimism"];
    let urls = ["https://indexer1.io/graphql", "https://indexer2.io/graphql", "https://indexer3.io/graphql"];
    let results = ["success", "error", "timeout"];
    let error_messages = ["", "timeout error", "server error", "validation error"];
    
    IndexerQueryProtobuf {
        indexer: Uuid::new_v4().to_string().into_bytes(),
        deployment: Uuid::new_v4().to_string().into_bytes(),
        allocation: Uuid::new_v4().to_string().into_bytes(),
        indexed_chain: indexed_chains.choose(&mut rng)
            .expect("Indexed chains array should not be empty")
            .to_string(),
        url: urls.choose(&mut rng)
            .expect("URLs array should not be empty")
            .to_string(),
        fee_grt: rng.gen_range(0.0001..0.1),
        response_time_ms: rng.gen_range(10..1000),
        seconds_behind: rng.gen_range(0..300),
        result: results.choose(&mut rng)
            .expect("Results array should not be empty")
            .to_string(),
        indexer_errors: error_messages.choose(&mut rng)
            .expect("Error messages array should not be empty")
            .to_string(),
        blocks_behind: rng.gen_range(0..1000),
    }
}
