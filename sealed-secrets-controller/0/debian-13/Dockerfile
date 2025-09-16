# Copyright Reliza Incorporated. 2019 - 2025. Licensed under the terms of MIT.
# SPDX-License-Identifier: MIT

FROM docker.io/library/debian:13.1-slim@sha256:c2880112cc5c61e1200c26f106e4123627b49726375eb5846313da9cca117337 AS builder

ARG DOWNLOADS_URL="downloads.bitnami.com/files/stacksmith"
ARG TARGETARCH

ENV OS_ARCH="${TARGETARCH:-amd64}"

COPY prebuildfs /
SHELL ["/bin/bash", "-o", "errexit", "-o", "nounset", "-o", "pipefail", "-c"]

# Install required system packages and dependencies
RUN install_packages ca-certificates curl
RUN --mount=type=secret,id=downloads_url,env=SECRET_DOWNLOADS_URL \
    DOWNLOADS_URL=${SECRET_DOWNLOADS_URL:-${DOWNLOADS_URL}} ; \
    mkdir -p /tmp/bitnami/pkg/cache/ ; cd /tmp/bitnami/pkg/cache/ || exit 1 ; \
    COMPONENTS=( \
      "sealed-secrets-0.32.2-0-linux-${OS_ARCH}-debian-12" \
    ) ; \
    for COMPONENT in "${COMPONENTS[@]}"; do \
      if [ ! -f "${COMPONENT}.tar.gz" ]; then \
        curl -SsLf "https://${DOWNLOADS_URL}/${COMPONENT}.tar.gz" -O ; \
        curl -SsLf "https://${DOWNLOADS_URL}/${COMPONENT}.tar.gz.sha256" -O ; \
      fi ; \
      sha256sum -c "${COMPONENT}.tar.gz.sha256" ; \
      tar -zxf "${COMPONENT}.tar.gz" -C /opt/bitnami --strip-components=2 --no-same-owner ; \
      rm -rf "${COMPONENT}".tar.gz{,.sha256} ; \
    done
RUN uninstall_packages curl

######

FROM scratch

ARG DOWNLOADS_URL="downloads.bitnami.com/files/stacksmith"
ARG TARGETARCH

ENV OS_ARCH="${TARGETARCH:-amd64}"


ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned

COPY rootfs /
COPY --from=builder /opt/bitnami/sealed-secrets/.spdx-sealed-secrets.spdx /opt/bitnami/sealed-secrets/.spdx-sealed-secrets.spdx
COPY --from=builder /opt/bitnami/sealed-secrets/bin/controller /opt/bitnami/sealed-secrets/bin/controller
COPY --from=builder /opt/bitnami/sealed-secrets/licenses /opt/bitnami/sealed-secrets/licenses

ENV APP_VERSION="0.32.2" \
    BITNAMI_APP_NAME="sealed-secrets-controller" \
    IMAGE_REVISION="0" \
    PATH="/opt/bitnami/sealed-secrets/bin:$PATH"

LABEL org.opencontainers.image.base.name="scratch"
LABEL org.opencontainers.image.revision=$GIT_COMMIT
LABEL git_branch=$GIT_BRANCH
LABEL ci_environment=$CI_ENV
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.vendor="Reliza"
LABEL org.opencontainers.image.title="Sealed Secrets Controller"
LABEL sealed_secrets_controller_version="0.32.2"


USER 1001

ENTRYPOINT [ "controller" ]
