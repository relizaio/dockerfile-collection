#!/bin/sh
set -o errexit
set -o nounset
set -o pipefail
TIMESTAMP=$(date +"%Y-%m-%d-%H-%M")
DUMP_FILE="/tmp/dump.gz"

/usr/bin/pg_dump -U $PG_USER -h $PG_HOST $PG_DATABASE -Fc | /bin/gzip > "$DUMP_FILE"

if [ -n "${ENCRYPTION_PASSWORD:-}" ]; then
    openssl enc -aes-256-cbc -a -pbkdf2 -iter 600000 -salt -pass pass:"$ENCRYPTION_PASSWORD" -in "$DUMP_FILE" -out "${DUMP_FILE}.enc"
    /usr/local/bin/aws s3 cp "${DUMP_FILE}.enc" "s3://$AWS_BUCKET/$DUMP_PREFIX-${TIMESTAMP}.gz.enc"
else
    /usr/local/bin/aws s3 cp "$DUMP_FILE" "s3://$AWS_BUCKET/$DUMP_PREFIX-${TIMESTAMP}.gz"
fi