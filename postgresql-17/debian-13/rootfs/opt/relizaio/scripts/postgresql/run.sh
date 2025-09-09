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
. /opt/relizaio/scripts/libos.sh

# Load PostgreSQL environment variables
. /opt/relizaio/scripts/postgresql-env.sh

flags=("-D" "$POSTGRESQL_DATA_DIR" "--config-file=$POSTGRESQL_CONF_FILE" "--external_pid_file=$POSTGRESQL_PID_FILE" "--hba_file=$POSTGRESQL_PGHBA_FILE")

if [[ -n "${POSTGRESQL_EXTRA_FLAGS:-}" ]]; then
    read -r -a extra_flags <<< "$POSTGRESQL_EXTRA_FLAGS"
    flags+=("${extra_flags[@]}")
fi

if [[ -n "${POSTGRESQL_DEFAULT_TRANSACTION_ISOLATION:-}" ]]; then
    flags+=("-c" "default_transaction_isolation=$POSTGRESQL_DEFAULT_TRANSACTION_ISOLATION")
fi

flags+=("$@")

cmd=$(command -v postgres)

info "** Starting PostgreSQL **"
if am_i_root; then
    exec_as_user "$POSTGRESQL_DAEMON_USER" "$cmd" "${flags[@]}"
else
    exec "$cmd" "${flags[@]}"
fi
