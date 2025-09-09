#!/bin/bash
# Copyright Reliza Incorporated. 2019 - 2025. Licensed under the terms of AGPL-3.0-only.
# SPDX-License-Identifier: AGPL-3.0-only
#
# Relizaio custom library

# shellcheck disable=SC1091

# Load Generic Libraries
. /opt/relizaio/scripts/liblog.sh

# Constants
BOLD='\033[1m'

# Functions

########################
# Print the welcome page
# Globals:
#   DISABLE_WELCOME_MESSAGE
#   RELIZAIO_APP_NAME
# Arguments:
#   None
# Returns:
#   None
#########################
print_welcome_page() {
    if [[ -z "${DISABLE_WELCOME_MESSAGE:-}" ]]; then
        if [[ -n "$RELIZAIO_APP_NAME" ]]; then
            print_image_welcome_page
        fi
    fi
}

########################
# Print the welcome page for a Relizaio Docker image
# Globals:
#   RELIZAIO_APP_NAME
# Arguments:
#   None
# Returns:
#   None
#########################
print_image_welcome_page() {
    local github_url="https://github.com/relizaio/dockerfile-collection"

    info ""
    info "${BOLD}Welcome to the Relizaio ${RELIZAIO_APP_NAME} container${RESET}"
    info ""
}

