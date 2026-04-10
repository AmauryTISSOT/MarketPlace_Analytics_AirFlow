#!/bin/bash
set -e

BUCKET="local/marketplace-raw"
TODAY=$(date +%Y-%m-%d)
KEEP_CONTENT="{\"init\": true, \"created_at\": \"${TODAY}\"}"

echo "Attente de MinIO..."
until mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null; do
    sleep 2
done

mc mb "${BUCKET}" --ignore-existing
echo "Bucket 'marketplace-raw' cree."

create_prefix() {
    local path="$1"
    echo "${KEEP_CONTENT}" | mc pipe "${BUCKET}/${path}/.keep"
    echo "Prefixe '${path}/' initialise."
}

create_prefix "orders/dt=${TODAY}" &
create_prefix "products" &
create_prefix "sellers" &
create_prefix "customers" &
wait

echo ""
mc ls --recursive "${BUCKET}/"
echo "Initialisation MinIO MarketPlace terminee."
