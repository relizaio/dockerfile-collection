#!/bin/sh
set -o errexit
set -o nounset
set -o pipefail
TIMESTAMP=$(date +"%Y-%m-%d-%H-%M")

oras login -u "$REGISTRY_USERNAME" -p "$REGISTRY_TOKEN" "$REGISTRY_HOST"
oras backup --output /tmp/dump.tar "$REGISTRY_HOST/$REGISTRY_PATH"
/bin/gzip /tmp/dump.tar

if [ -n "${ENCRYPTION_PASSWORD:-}" ]; then
    openssl enc -aes-256-cbc -a -pbkdf2 -iter 600000 -salt -pass pass:"$ENCRYPTION_PASSWORD" -in /tmp/dump.tar.gz -out /tmp/dump.tar.gz.enc
    /usr/local/bin/aws s3 cp "/tmp/dump.tar.gz.enc" "s3://$AWS_BUCKET/$DUMP_PREFIX-${TIMESTAMP}.tar.gz.enc"
else
    /usr/local/bin/aws s3 cp "/tmp/dump.tar.gzc" "s3://$AWS_BUCKET/$DUMP_PREFIX-${TIMESTAMP}.tar.gz"
fi