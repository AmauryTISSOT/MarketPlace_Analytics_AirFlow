#!/bin/sh

BUCKET="local/marketplace-raw"
TODAY=$(date +%Y-%m-%d)

until /usr/bin/mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null; do
    sleep 2
done

/usr/bin/mc mb "${BUCKET}" --ignore-existing

echo '{}' | /usr/bin/mc pipe "${BUCKET}/orders/dt=${TODAY}/.keep"
echo '{}' | /usr/bin/mc pipe "${BUCKET}/products/.keep"
echo '{}' | /usr/bin/mc pipe "${BUCKET}/sellers/.keep"

/usr/bin/mc ls --recursive "${BUCKET}/"
