#!/bin/sh
set -e

BUCKET="local/marketplace-raw"
TODAY=$(date +%Y-%m-%d)

until mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null; do
    sleep 2
done

mc mb "${BUCKET}" --ignore-existing

echo '{}' | mc pipe "${BUCKET}/orders/dt=${TODAY}/.keep"
echo '{}' | mc pipe "${BUCKET}/products/.keep"
echo '{}' | mc pipe "${BUCKET}/sellers/.keep"

mc ls --recursive "${BUCKET}/"