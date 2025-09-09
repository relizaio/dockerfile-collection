#!/bin/bash
# Copyright Reliza Incorporated. 2019 - 2025. Licensed under the terms of AGPL-3.0-only.
# SPDX-License-Identifier: AGPL-3.0-only

# shellcheck disable=SC1091

set -o errexit
set -o nounset
set -o pipefail
# set -o xtrace # Uncomment this line for debugging purposes

# Load libraries
. /opt/relizaio/scripts/libpostgresql.sh
. /opt/relizaio/scripts/libautoctl.sh
. /opt/relizaio/scripts/libos.sh

# Load PostgreSQL environment variables
. /opt/relizaio/scripts/postgresql-env.sh

export HOME="$POSTGRESQL_AUTOCTL_VOLUME_DIR"

autoctl_initialize

flags=("run" "--pgdata" "$POSTGRESQL_DATA_DIR")
cmd=$(command -v pg_autoctl)

info "** Starting PostgreSQL autoctl_node (Mode: $POSTGRESQL_AUTOCTL_MODE) **"
if am_i_root; then
    exec_as_user "$POSTGRESQL_DAEMON_USER" "$cmd" "${flags[@]}"
else
    PGPASSWORD=$POSTGRESQL_REPLICATION_PASSWORD exec "$cmd" "${flags[@]}"
fi
