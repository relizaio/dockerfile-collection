#!/bin/sh
/usr/bin/pg_dump -U $PG_USER -h $PG_HOST $PG_DATABASE | /bin/gzip > /tmp/dump.gz && /usr/local/bin/aws s3 cp /tmp/dump.gz s3://$AWS_BUCKET/$DUMP_PREFIX-$(date +"%Y-%m-%d-%H-%M").gz