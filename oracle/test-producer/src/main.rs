use std::env;
use std::time::Duration;

use hex;
use prost::Message;
use rand::Rng;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use tokio::time;
use once_cell::sync::Lazy;
use rand::seq::SliceRandom;
use num_cpus;

// Mock types to match the gateway's dependencies
mod mock {
    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    pub struct DeploymentId(pub [u8; 32]);

    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    pub struct IndexerId(pub [u8; 20]);

    #[derive(Clone)]
    pub struct SubgraphId(pub String);

    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    pub struct Address(pub [u8; 20]);

    impl std::ops::Deref for IndexerId {
        type Target = Address;
        fn deref(&self) -> &Self::Target {
            unsafe { std::mem::transmute(self) }
        }
    }

    impl DeploymentId {
        pub fn to_vec(&self) -> Vec<u8> {
            self.0.to_vec()
        }
    }

    impl IndexerId {
        pub fn to_vec(&self) -> Vec<u8> {
            self.0.to_vec()
        }
    }

    impl SubgraphId {
        pub fn to_string(&self) -> String {
            self.0.clone()
        }
    }

    impl Address {
        pub fn to_vec(&self) -> Vec<u8> {
            self.0.to_vec()
        }
    }

    // Mock error types
    pub enum Error {
        QueryFailed(String),
    }

    impl ToString for Error {
        fn to_string(&self) -> String {
            match self {
                Error::QueryFailed(reason) => format!("Query failed: {}", reason),
            }
        }
    }

    pub enum IndexerError {
        NetworkError(String),
        Timeout,
    }

    impl ToString for IndexerError {
        fn to_string(&self) -> String {
            match self {
                IndexerError::NetworkError(err) => format!("Network error: {}", err),
                IndexerError::Timeout => "Timeout".to_string(),
            }
        }
    }

    // Mock Receipt
    pub struct Receipt {
        allocation: Address,
        value: u64,
    }

    impl Receipt {
        pub fn new(allocation: Address, value: u64) -> Self {
            Self { allocation, value }
        }

        pub fn allocation(&self) -> Address {
            self.allocation
        }

        pub fn value(&self) -> u64 {
            self.value
        }
    }

    // Simplified IndexerResponse without attestation
    pub struct IndexerResponse {
        pub errors: Vec<String>,
    }
}

use mock::*;

// Protobuf definitions - exact copies from the gateway code
#[derive(prost::Message)]
pub struct ClientQueryProtobuf {
    #[prost(string, tag = "1")]
    pub gateway_id: String,
    // 20 bytes
    #[prost(bytes, tag = "2")]
    pub receipt_signer: Vec<u8>,
    #[prost(string, tag = "3")]
    pub query_id: String,
    #[prost(string, tag = "4")]
    pub api_key: String,
    #[prost(string, tag = "5")]
    pub result: String,
    #[prost(uint32, tag = "6")]
    pub response_time_ms: u32,
    #[prost(uint32, tag = "7")]
    pub request_bytes: u32,
    #[prost(uint32, optional, tag = "8")]
    pub response_bytes: Option<u32>,
    #[prost(double, tag = "9")]
    pub total_fees_usd: f64,
    #[prost(message, repeated, tag = "10")]
    pub indexer_queries: Vec<IndexerQueryProtobuf>,
    #[prost(string, tag = "11")]
    pub user_id: String,
    #[prost(string, optional, tag = "12")]
    pub subgraph: Option<String>,
}

#[derive(prost::Message)]
pub struct IndexerQueryProtobuf {
    /// 20 bytes
    #[prost(bytes, tag = "1")]
    pub indexer: Vec<u8>,
    /// 32 bytes
    #[prost(bytes, tag = "2")]
    pub deployment: Vec<u8>,
    /// 20 bytes
    #[prost(bytes, tag = "3")]
    pub allocation: Vec<u8>,
    #[prost(string, tag = "4")]
    pub indexed_chain: String,
    #[prost(string, tag = "5")]
    pub url: String,
    #[prost(double, tag = "6")]
    pub fee_grt: f64,
    #[prost(uint32, tag = "7")]
    pub response_time_ms: u32,
    #[prost(uint32, tag = "8")]
    pub seconds_behind: u32,
    #[prost(string, tag = "9")]
    pub result: String,
    #[prost(string, tag = "10")]
    pub indexer_errors: String,
    #[prost(uint64, tag = "11")]
    pub blocks_behind: u64,
}

// Simplified structures to match the gateway's domain model without attestation
pub struct ClientRequest {
    pub id: String,
    pub response_time_ms: u16,
    pub result: Result<(), Error>,
    pub api_key: String,
    pub user: String,
    pub subgraph: Option<SubgraphId>,
    pub grt_per_usd: f64, // Using f64 instead of NotNan<f64> for simplicity
    pub indexer_requests: Vec<IndexerRequest>,
    pub request_bytes: u32,
    pub response_bytes: Option<u32>,
}

pub struct IndexerRequest {
    pub indexer: IndexerId,
    pub deployment: DeploymentId,
    pub url: String,
    pub receipt: Receipt,
    pub subgraph_chain: String,
    pub result: Result<IndexerResponse, IndexerError>,
    pub response_time_ms: u16,
    pub seconds_behind: u32,
    pub blocks_behind: u64,
}

// Helper functions to generate random data
fn random_bytes(len: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut bytes = vec![0u8; len];
    rng.fill(&mut bytes[..]);
    bytes
}

// Generate a random string that's guaranteed to be valid UTF-8
fn random_utf8_string(prefix: &str, len: usize) -> String {
    use rand::{thread_rng, Rng};

    let suffix: String = thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(len)
        .map(char::from)
        .collect();

    format!("{}-{}", prefix, suffix)
}

// Generate random hex string (guaranteed valid UTF-8) from random bytes
fn random_hex_string(len: usize) -> String {
    hex::encode(random_bytes(len))
}

// --- Define Pool Sizes ---
const POOL_SIZE: usize = 30; // Number of unique IDs for each type

// --- Define Static ID Pools (Place this before the random_* functions) ---

static INDEXER_ID_POOL: Lazy<Vec<IndexerId>> = Lazy::new(|| {
    (0..POOL_SIZE)
        .map(|_| {
            let mut bytes = [0u8; 20];
            rand::thread_rng().fill(&mut bytes);
            IndexerId(bytes)
        })
        .collect()
});

static DEPLOYMENT_ID_POOL: Lazy<Vec<DeploymentId>> = Lazy::new(|| {
    (0..POOL_SIZE)
        .map(|_| {
            let mut bytes = [0u8; 32];
            rand::thread_rng().fill(&mut bytes);
            DeploymentId(bytes)
        })
        .collect()
});

// Pool for Addresses (used by random_address and tap_signer)
static ADDRESS_POOL: Lazy<Vec<Address>> = Lazy::new(|| {
    (0..POOL_SIZE)
        .map(|_| {
            let mut bytes = [0u8; 20];
            rand::thread_rng().fill(&mut bytes);
            Address(bytes)
        })
        .collect()
});

// Pool for Subgraph IDs (derived from Deployment pool for consistency)
static SUBGRAPH_ID_POOL: Lazy<Vec<SubgraphId>> = Lazy::new(|| {
    DEPLOYMENT_ID_POOL
        .iter()
        .map(|d| SubgraphId(format!("Qm{}", hex::encode(d.0))))
        .collect()
});

// Pools for String-based IDs
static GATEWAY_ID_POOL: Lazy<Vec<String>> = Lazy::new(|| {
    (0..POOL_SIZE)
        .map(|i| format!("gateway-pool-{}", i))
        .collect()
});

static API_KEY_POOL: Lazy<Vec<String>> = Lazy::new(|| {
    (0..POOL_SIZE)
        .map(|i| random_utf8_string(&format!("api-key-pool-{}", i), 10))
        .collect()
});

static USER_POOL: Lazy<Vec<String>> = Lazy::new(|| {
    (0..POOL_SIZE)
        .map(|i| random_utf8_string(&format!("user-pool-{}", i), 10))
        .collect()
});

// --- Update the random ID generation functions ---

// Replace the existing random_address function (lines 223-227)
fn random_address() -> Address {
    // Choose a random Address from the pre-generated pool
    *ADDRESS_POOL.choose(&mut rand::thread_rng()).unwrap()
}

// Replace the existing random_deployment_id function (lines 229-233)
fn random_deployment_id() -> DeploymentId {
    // Choose a random DeploymentId from the pre-generated pool
    *DEPLOYMENT_ID_POOL.choose(&mut rand::thread_rng()).unwrap()
}

// Replace the existing random_indexer_id function (lines 235-239)
fn random_indexer_id() -> IndexerId {
    // Choose a random IndexerId from the pre-generated pool
    *INDEXER_ID_POOL.choose(&mut rand::thread_rng()).unwrap()
}

// --- Add/Update functions for String-based IDs ---

// Add this function to select from the Subgraph ID pool
fn random_subgraph_id() -> SubgraphId {
     SUBGRAPH_ID_POOL.choose(&mut rand::thread_rng()).unwrap().clone()
}

// Add this function to select from the Gateway ID pool
fn random_gateway_id() -> String {
     GATEWAY_ID_POOL.choose(&mut rand::thread_rng()).unwrap().clone()
}

// Add this function to select from the API Key pool
fn random_api_key() -> String {
     API_KEY_POOL.choose(&mut rand::thread_rng()).unwrap().clone()
}

// Add this function to select from the User pool
fn random_user() -> String {
     USER_POOL.choose(&mut rand::thread_rng()).unwrap().clone()
}

// --- Update generate_random_client_request (lines 241-314) ---
// Modify calls inside this function to use the new pool-based random functions

fn generate_random_client_request() -> ClientRequest {
    let mut rng = rand::thread_rng();

    let num_indexers = rng.gen_range(1..5);
    let mut indexer_requests = Vec::with_capacity(num_indexers);

    // --- Key Change: Pick IDs *once* per ClientRequest where appropriate ---
    let deployment_id = random_deployment_id(); // Pick one deployment from pool
    let subgraph_id = random_subgraph_id(); // Pick one subgraph from pool (could also derive from deployment)
    let api_key = random_api_key(); // Pick one api key from pool
    let user = random_user(); // Pick one user from pool

    let subgraph_chain = if rng.gen_bool(0.8) { "mainnet".to_string() } else { "goerli".to_string() };

    for _ in 0..num_indexers {
        let success = rng.gen_bool(0.9);

        // --- Use pool-based functions for indexer-specific IDs ---
        let indexer_id = random_indexer_id();
        let allocation_id = random_address(); // Allocation can vary per indexer request

        let result = if success {
            Ok(IndexerResponse { errors: vec![] })
        } else {
            Err(IndexerError::NetworkError(
                "Error processing request".to_string(),
            ))
        };

        indexer_requests.push(IndexerRequest {
            indexer: indexer_id, // From pool
            deployment: deployment_id, // Use the *same* deployment for all indexers in this request
            url: format!( // Use consistent subgraph ID for URL
                "https://api.thegraph.com/subgraphs/id/{}",
                 subgraph_id.0 // Access the inner String
            ),
            receipt: Receipt::new(
                allocation_id, // From pool
                rng.gen_range(100_000_000_000_000..1_000_000_000_000_000),
            ),
            subgraph_chain: subgraph_chain.clone(),
            result,
            response_time_ms: rng.gen_range(50..2000) as u16,
            seconds_behind: rng.gen_range(0..100),
            blocks_behind: rng.gen_range(0..50),
        });
    }

    let success = rng.gen_bool(0.95);

    ClientRequest {
        id: random_hex_string(16), // Keep request ID unique
        response_time_ms: rng.gen_range(50..5000) as u16,
        result: if success {
            Ok(())
        } else {
            Err(Error::QueryFailed("Query validation error".to_string()))
        },
        api_key: api_key, // Use the chosen API key
        user: user,       // Use the chosen user
        subgraph: if rng.gen_bool(0.9) { Some(subgraph_id) } else { None }, // Use the chosen subgraph ID
        grt_per_usd: rng.gen_range(0.05..0.2),
        indexer_requests,
        request_bytes: rng.gen_range(100..5000),
        response_bytes: if rng.gen_bool(0.95) {
            Some(rng.gen_range(200..10000))
        } else {
            None
        },
    }
}

// Function that matches the logic in gateway's report function but without attestation
fn encode_client_request(
    client_request: ClientRequest,
    // tap_signer: Address, // Remove this parameter
    // graph_env: String, // Remove this parameter
) -> Vec<u8> {
    // --- Key Change: Select gateway and signer from pools here ---
    let gateway_id = random_gateway_id(); // Select from pool
    let tap_signer = random_address(); // Select from pool

    let indexer_queries = client_request
        .indexer_requests
        .iter()
        .map(|indexer_request| IndexerQueryProtobuf {
            indexer: indexer_request.indexer.to_vec(),
            deployment: indexer_request.deployment.to_vec(),
            allocation: indexer_request.receipt.allocation().to_vec(),
            indexed_chain: indexer_request.subgraph_chain.clone(),
            url: indexer_request.url.clone(),
            fee_grt: indexer_request.receipt.value() as f64 * 1e-18,
            response_time_ms: indexer_request.response_time_ms as u32,
            seconds_behind: indexer_request.seconds_behind,
            result: indexer_request
                .result
                .as_ref()
                .map(|_| "success".to_string())
                .unwrap_or_else(|err| err.to_string()),
            indexer_errors: indexer_request
                .result
                .as_ref()
                .map(|r| {
                    r.errors
                        .iter()
                        .map(|err| err.as_str())
                        .collect::<Vec<&str>>()
                        .join("; ")
                })
                .unwrap_or_default(),
            blocks_behind: indexer_request.blocks_behind,
        })
        .collect();

    let total_fees_grt: f64 = client_request
        .indexer_requests
        .iter()
        .map(|i| i.receipt.value() as f64 * 1e-18)
        .sum();
    let total_fees_usd: f64 = total_fees_grt / client_request.grt_per_usd;

    let client_query_msg = ClientQueryProtobuf {
        gateway_id,
        receipt_signer: tap_signer.to_vec(),
        query_id: client_request.id,
        api_key: client_request.api_key,
        user_id: client_request.user,
        subgraph: client_request.subgraph.map(|s| s.to_string()),
        result: client_request
            .result
            .map(|()| "success".to_string())
            .unwrap_or_else(|err| err.to_string()),
        response_time_ms: client_request.response_time_ms as u32,
        request_bytes: client_request.request_bytes,
        response_bytes: client_request.response_bytes,
        total_fees_usd,
        indexer_queries,
    };

    // Encode to Protobuf
    let mut buf = Vec::new();
    client_query_msg.encode(&mut buf).unwrap();

    // Return the raw protobuf message without length prefix
    buf
}

// --- NEW: Asynchronous function to run in each producer task ---
async fn produce_messages(producer: FutureProducer, topic: String, task_id: u32, delay_micros: u64) {
    let mut counter: u64 = 0;
    println!("[Task {}] Starting producer loop", task_id);

    loop {
        // --- Use existing message generation logic ---
        let client_request = generate_random_client_request();
        let payload = encode_client_request(client_request);
        // --- End Use existing message generation logic ---

        // Use a simple counter or a field from the request for the key
        // to help distribute messages across partitions.
        // Using modulo is okay for basic distribution.
        let key = format!("key-{}", counter % 100);

        // Send asynchronously - *DO NOT* await the future here in the loop!
        let send_future = producer.send_result(
            FutureRecord::to(&topic)
                .payload(&payload)
                .key(&key),
        );

        // Check for immediate queuing errors (e.g., queue full).
        if let Err((e, _)) = send_future {
            eprintln!("[Task {}] Failed to queue message (queue full?): {:?}. Pausing...", task_id, e);
            // Pause briefly if the producer queue is full to avoid overwhelming it.
            time::sleep(Duration::from_millis(100)).await;
            // Optionally: break or implement more robust backoff
        }

        counter += 1;

        // Apply throttling delay if configured
        if delay_micros > 0 {
            time::sleep(Duration::from_micros(delay_micros)).await;
        } else {
            // Yield control occasionally if the loop is extremely tight (no throttling)
            if counter % 1000 == 0 {
                tokio::task::yield_now().await;
            }
        }
    }
}

// --- NEW: Updated main function using Tokio ---
#[tokio::main]
async fn main() {
    // Print librdkafka version
    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    // --- Configuration ---
    // Read target total messages per second (informational, not used for delay)
    let total_mps: u64 = env::var("MESSAGES_PER_SECOND")
        .unwrap_or_else(|_| "10000".to_string()) // Default to 10k
        .parse()
        .expect("MESSAGES_PER_SECOND must be a valid number");

    // Determine number of producer tasks (use logical cores)
    let num_tasks = num_cpus::get().max(1);
    println!("Target MPS: {}, Spawning {} producer tasks", total_mps, num_tasks);

    // Calculate delay per task to achieve target MPS
    // Each task needs to wait `num_tasks` times longer than the overall target interval
    let delay_per_task_micros = if total_mps > 0 {
        (1_000_000 * num_tasks as u64) / total_mps
    } else {
        0 // No delay if target MPS is 0 (run as fast as possible)
    };
    if delay_per_task_micros > 0 {
        println!(
            "[Config] Throttling enabled: Each task will pause for {} microseconds.",
            delay_per_task_micros
        );
    } else {
        println!("[Config] Throttling disabled (MESSAGES_PER_SECOND=0 or not set appropriately). Running at max speed.");
    }

    let broker = env::var("KAFKA_BROKER").unwrap_or_else(|_| "redpanda:9092".to_string());
    let topic = "gateway_qos_topic".to_string(); // Ensure this matches ClickHouse

    println!("Connecting to Kafka broker at {}", broker);
    println!("Waiting 5 seconds for Redpanda to initialize...");
    time::sleep(Duration::from_secs(5)).await;

    // --- Producer Setup ---
    // Configure the Kafka producer client
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("message.timeout.ms", "30000") // Max time librdkafka tries to send one message
        .set("security.protocol", "PLAINTEXT")
        // .set("debug", "all") // Disable verbose debugging for performance testing
        .set("enable.idempotence", "false") // Disable for max throughput test (less overhead)
        .set("retries", "5")                // Retries for recoverable send failures
        .set("retry.backoff.ms", "100")     // Backoff between retries (reduced for faster recovery)
        .set("socket.timeout.ms", "10000")
        .set("socket.keepalive.enable", "true")
        // --- Batching Configuration (Crucial for Throughput) ---
        .set("linger.ms", "5") // Wait up to 5ms to gather messages into a batch
        .set("batch.size", "131072") // Batch size in bytes (e.g., 128 KB). Tune based on avg message size.
        // --- Buffering Configuration (Increase if producer blocks/errors often) ---
        .set("queue.buffering.max.messages", "500000") // Max messages in internal producer queue
        .set("queue.buffering.max.ms", "1000") // Max time msgs wait in queue before send() blocks/errors
        // --- Optional: Compression (Reduces network bandwidth, increases CPU) ---
        // .set("compression.codec", "snappy") // or lz4, gzip, zstd
        .create()
        .expect("Producer creation error");

    // --- Spawn Producer Tasks ---
    let mut tasks = vec![];
    println!("Spawning {} producer tasks...", num_tasks);
    for i in 0..num_tasks {
        let producer_clone = producer.clone(); // Clone producer for each task
        let topic_clone = topic.clone();
        tasks.push(tokio::spawn(produce_messages(
            producer_clone,
            topic_clone,
            i as u32,
            delay_per_task_micros, // Pass the calculated delay
        )));
    }

    // --- Keep Main Task Alive ---
    println!(
        "Producer tasks running. Sending to topic '{}'. Press Ctrl+C to stop.",
        topic
    );
    // Wait for all tasks to complete (they run infinitely, so this waits forever unless they error out)
    for task in tasks {
        if let Err(e) = task.await {
            eprintln!("Producer task failed: {:?}", e);
        }
    }
}
