#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
# Define paths
TEMPLATE_DIR="/etc/clickhouse-initdb-templates" # Directory where template is mounted
INITDB_DIR="/docker-entrypoint-initdb.d"      # Standard ClickHouse init directory
TEMPLATE_FILE="${TEMPLATE_DIR}/init-db.sql.template"
OUTPUT_SQL_FILE="${INITDB_DIR}/init-db.sql"

# --- Check Required Environment Variables ---
: "${KAFKA_BROKER_LIST?Need to set KAFKA_BROKER_LIST}"
: "${KAFKA_GROUP_NAME?Need to set KAFKA_GROUP_NAME}"

# --- Perform Substitution using sed ---
echo "Substituting Kafka configuration using sed into ${TEMPLATE_FILE} -> ${OUTPUT_SQL_FILE}"

# Create the output directory if it doesn't exist
mkdir -p "${INITDB_DIR}"

# Use sed for substitution. Using '|' as delimiter to avoid issues with '/' in paths/brokers.
sed -e "s|\${KAFKA_BROKER_LIST}|${KAFKA_BROKER_LIST}|g" \
    -e "s|\${KAFKA_GROUP_NAME}|${KAFKA_GROUP_NAME}|g" \
    "${TEMPLATE_FILE}" > "${OUTPUT_SQL_FILE}"

echo "Substitution complete. SQL file generated in ${INITDB_DIR}."
echo "Executing default ClickHouse entrypoint: /entrypoint.sh $@"

# --- Execute Original Entrypoint ---
# Pass along any arguments originally intended for the ClickHouse entrypoint
exec /entrypoint.sh "$@" 