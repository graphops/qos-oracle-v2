use std::env;
use std::time::Duration;

use hex;
use prost::Message;
use rand::Rng;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use tokio::time;

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

fn random_address() -> Address {
    let mut bytes = [0u8; 20];
    rand::thread_rng().fill(&mut bytes);
    Address(bytes)
}

fn random_deployment_id() -> DeploymentId {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill(&mut bytes);
    DeploymentId(bytes)
}

fn random_indexer_id() -> IndexerId {
    let mut bytes = [0u8; 20];
    rand::thread_rng().fill(&mut bytes);
    IndexerId(bytes)
}

fn generate_random_client_request() -> ClientRequest {
    let mut rng = rand::thread_rng();

    // Generate between 1 and 4 indexer requests
    let num_indexers = rng.gen_range(1..5);
    let mut indexer_requests = Vec::with_capacity(num_indexers);

    for _ in 0..num_indexers {
        let success = rng.gen_bool(0.9); // 90% success rate

        let indexer_id = random_indexer_id();
        let allocation = random_address();

        let result = if success {
            Ok(IndexerResponse { errors: vec![] })
        } else {
            // Use safe error types with valid UTF-8
            Err(IndexerError::NetworkError(
                "Error processing request".to_string(),
            ))
        };

        indexer_requests.push(IndexerRequest {
            indexer: indexer_id,
            deployment: random_deployment_id(),
            // Ensure URL is valid UTF-8
            url: format!(
                "https://api.thegraph.com/subgraphs/id/Qm{}",
                random_hex_string(10)
            ),
            receipt: Receipt::new(
                allocation,
                rng.gen_range(100_000_000_000_000..1_000_000_000_000_000),
            ),
            // Ensure chain name is valid UTF-8
            subgraph_chain: "mainnet".to_string(),
            result,
            response_time_ms: rng.gen_range(50..2000) as u16,
            seconds_behind: rng.gen_range(0..100),
            blocks_behind: rng.gen_range(0..50),
        });
    }

    let success = rng.gen_bool(0.95); // 95% success rate for overall query

    ClientRequest {
        // Use random hex for IDs (guaranteed valid UTF-8)
        id: random_hex_string(16),
        response_time_ms: rng.gen_range(50..5000) as u16,
        result: if success {
            Ok(())
        } else {
            // Use predefined error message to ensure UTF-8
            Err(Error::QueryFailed("Query validation error".to_string()))
        },
        // Use random alphanumeric strings with prefixes
        api_key: random_utf8_string("api", 16),
        user: random_utf8_string("user", 16),
        subgraph: if rng.gen_bool(0.9) {
            // Guarantee valid UTF-8 for subgraph ID
            Some(SubgraphId(format!("Qm{}", random_hex_string(10))))
        } else {
            None
        },
        grt_per_usd: rng.gen_range(0.05..0.2), // Realistic GRT/USD price range
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
    tap_signer: Address,
    graph_env: String,
) -> Vec<u8> {
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
        gateway_id: graph_env,
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
    let broker = env::var("KAFKA_BROKER").unwrap_or_else(|_| "redpanda:9092".to_string());

    println!("Connecting to Kafka broker at {}", broker);

    // Wait for Redpanda to be fully ready
    println!("Waiting 5 seconds for Redpanda to initialize...");
    time::sleep(Duration::from_secs(5)).await;

    // Create a Kafka producer with explicit PLAINTEXT protocol
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("message.timeout.ms", "30000")
        .set("security.protocol", "PLAINTEXT")
        .set("debug", "all")
        .set("enable.idempotence", "false") // Simplify for testing
        .set("retries", "5") // Retry a few times
        .set("retry.backoff.ms", "1000") // 1 second between retries
        .set("socket.timeout.ms", "10000") // 10 seconds socket timeout
        .set("socket.keepalive.enable", "true")
        .create()
        .expect("Producer creation error");

    // Make sure this matches the topic name in ClickHouse Kafka engine
    let topic = "gateway_qos_topic";
    let mut counter = 0;

    // Create a fixed tap_signer and graph_env to use for all messages
    let tap_signer = random_address();
    // Ensure graph_env is valid UTF-8
    let graph_env = "testnet-gateway".to_string();

    loop {
        // Generate a random client request
        let client_request = generate_random_client_request();

        // Encode using the gateway's exact logic
        let encoded_message = encode_client_request(client_request, tap_signer, graph_env.clone());

        println!("Encoded message: {} ", hex::encode(&encoded_message));
        println!("Encoded message size: {} bytes", encoded_message.len());

        counter += 1;

        match producer
            .send(
                FutureRecord::to(topic)
                    .payload(&encoded_message)
                    .key(&format!("{}", counter)),
                Duration::from_secs(0),
            )
            .await
        {
            Ok((partition, offset)) => println!("Delivered: ({}, {})", partition, offset),
            Err((e, _)) => eprintln!("Delivery error: {:?}", e),
        }
        // Sleep for the configured delay
        time::sleep(delay).await;
    }
}
