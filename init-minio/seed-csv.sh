#!/bin/bash
# Seed MinIO avec des fichiers CSV pour le Sujet B

set -e

echo "Attente de MinIO..."
until mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null; do
    sleep 2
done

mc mb local/data-lake --ignore-existing
echo "Bucket data-lake cree"

TODAY=$(date +%Y-%m-%d)

CSV_PATH="/tmp/events_${TODAY}.csv"
cat > "$CSV_PATH" << CSVEOF
id,user_id,event_type,page,duration_ms,device,created_at
1,3,page_view,/home,234,mobile,${TODAY}T08:12:00
2,7,click,/products,567,desktop,${TODAY}T09:05:00
3,1,purchase,/checkout,1200,mobile,${TODAY}T10:30:00
4,12,page_view,/about,89,tablet,${TODAY}T11:15:00
5,5,signup,/register,3400,desktop,${TODAY}T12:00:00
6,9,page_view,/home,456,mobile,${TODAY}T13:22:00
7,2,click,/products,321,desktop,${TODAY}T14:10:00
8,15,purchase,/checkout,890,mobile,${TODAY}T15:45:00
9,8,logout,/account,120,tablet,${TODAY}T16:30:00
10,4,page_view,/blog,678,desktop,${TODAY}T17:00:00
CSVEOF
mc cp "$CSV_PATH" "local/data-lake/landing/csv/events_${TODAY}.csv"
echo "CSV depose : landing/csv/events_${TODAY}.csv"
echo "Seed MinIO termine"
