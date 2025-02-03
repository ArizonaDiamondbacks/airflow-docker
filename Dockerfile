# -----------------------------------------------------------
# 1) BUILDER STAGE: Build the Rust workspace from hawkeye_etl
# -----------------------------------------------------------
FROM rust:1.83-slim-bullseye AS builder

USER root

# Step 1: Install dependencies, including tree for debugging
RUN apt-get update && apt-get install -y clang wget ca-certificates tar tree

# Step 2: Download and extract mold (separated so we can debug if needed)
RUN wget https://github.com/rui314/mold/releases/download/v2.36.0/mold-2.36.0-x86_64-linux.tar.gz \
    && mkdir mold-temp \
    && tar -xzf mold-2.36.0-x86_64-linux.tar.gz -C mold-temp --strip-components=1


# Debug and verify that mold exists; if not, output directory contents and fail
RUN if [ -f mold-temp/bin/mold ]; then \
         echo "mold found in mold-temp/bin:"; \
         ls -la mold-temp/bin; \
     else \
         echo "ERROR: mold not found in mold-temp/bin. Contents:"; \
         ls -la mold-temp; \
         exit 1; \
     fi
    

# (Optional) Verify that mold exists
RUN test -f mold-temp/bin/mold

# Move mold into place, set permissions, and clean up
RUN mv mold-temp/bin/mold /usr/local/bin/ \
    && chmod +x /usr/local/bin/mold \
    && rm -rf mold-temp mold-2.36.0-x86_64-linux.tar.gz

# Set working directory for building
WORKDIR /app

# (Debug) List current /app structure before copying files
RUN tree -L 2 /app

# Copy Cargo configuration
COPY hawkeye_etl/.cargo/config.toml .cargo/config.toml

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
COPY --chmod=775 --from=builder /app/target/release/json_to_parquet /opt/airflow/bins/
COPY --chmod=775 --from=builder /app/target/release/upload_to_s3   /opt/airflow/bins/

# Make them executable
RUN chmod 775 /opt/airflow/bins/json_to_parquet \
                /opt/airflow/bins/upload_to_s3

USER airflow

# If you need extra Python packages
RUN pip install --no-cache-dir \
    boto3 \
    fastavro

    