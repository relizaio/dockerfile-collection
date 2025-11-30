#!/bin/sh
set -o errexit
set -o nounset
set -o pipefail
/usr/bin/pg_dump -U $PG_USER -h $PG_HOST $PG_DATABASE -Fc | /bin/gzip > /tmp/dump.gz && /usr/local/bin/aws s3 cp /tmp/dump.gz s3://$AWS_BUCKET/$DUMP_PREFIX-$(date +"%Y-%m-%d-%H-%M").gz