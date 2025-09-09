#!/bin/bash
# Copyright Reliza Incorporated. 2019 - 2025. Licensed under the terms of AGPL-3.0-only.
# SPDX-License-Identifier: AGPL-3.0-only
#
# Library to use for scripts expected to be used as Kubernetes lifecycle hooks

# shellcheck disable=SC1091

# Load generic libraries
. /opt/relizaio/scripts/liblog.sh
. /opt/relizaio/scripts/libos.sh

# Override functions that log to stdout/stderr of the current process, so they print to process 1
for function_to_override in stderr_print debug_execute; do
    # Output is sent to output of process 1 and thus end up in the container log
    # The hook output in general isn't saved
    eval "$(declare -f "$function_to_override") >/proc/1/fd/1 2>/proc/1/fd/2"
done
