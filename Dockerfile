# -----------------------------------------------------------
# 1) BUILDER STAGE: Build the Rust workspace from hawkeye_etl
# -----------------------------------------------------------
    FROM rust:slim-bullseye AS builder

    # We'll work in /app for building
    WORKDIR /app
    
    # First copy top-level workspace files (Cargo.toml, Cargo.lock) to cache dependencies
    COPY hawkeye_etl/Cargo.toml  ./
    COPY hawkeye_etl/Cargo.lock ./
    
    # If you have multiple crates inside hawkeye_etl, you can copy them individually
    COPY hawkeye_etl/json_to_parquet/Cargo.toml json_to_parquet/
    COPY hawkeye_etl/upload_to_s3/Cargo.toml     upload_to_s3/
    COPY hawkeye_etl/tests/Cargo.toml     tests/
    
    # Now copy the actual source code
    COPY hawkeye_etl/json_to_parquet/ json_to_parquet/
    COPY hawkeye_etl/upload_to_s3/     upload_to_s3/
    COPY hawkeye_etl/tests/     tests/
    
    # Build all binaries in release mode
    # This places them in /app/target/release/ inside the builder container
    RUN cargo build --release --workspace
    
    
    # ------------------------------------------------------------
    # 2) FINAL STAGE: Apache Airflow image with the compiled bins
    # ------------------------------------------------------------
    FROM apache/airflow:2.10.4-python3.9
    
    # Switch to root so we can copy files and set permissions
    USER root
    
    # Create a directory for Rust binaries
    RUN mkdir -p /opt/airflow/bins
    
    # Copy the two binaries from the builder
    COPY --from=builder /app/target/release/json_to_parquet /opt/airflow/bins/
    COPY --from=builder /app/target/release/upload_to_s3   /opt/airflow/bins/
    
    # Make them executable
    RUN chmod 775 /opt/airflow/bins/json_to_parquet \
                   /opt/airflow/bins/upload_to_s3
    
    USER airflow

    # If you need extra Python packages
    RUN pip install --no-cache-dir \
        boto3 \
        fastavro
    
    