fn main() {
    // Specify the path to the schema.proto file
    let proto_file = "./clickhouse/schema.proto";

    println!("cargo:rerun-if-changed={}", proto_file);

    prost_build::compile_protos(&[proto_file], &["."]).unwrap();
}
