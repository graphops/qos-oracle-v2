fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Try different locations for schema.proto
    let schema_paths = vec![
        "../clickhouse/schema.proto",
        "/usr/src/app/schema.proto",
        "schema.proto"
    ];
    
    // Try each path until one works
    for path in schema_paths {
        if std::path::Path::new(path).exists() {
            prost_build::compile_protos(&[path], &["../clickhouse/"])?;
            return Ok(());
        }
    }
    
    // If none of the paths worked, use the default one
    prost_build::compile_protos(&["../clickhouse/schema.proto"], &["../clickhouse/"])?;
    
    Ok(())
} 