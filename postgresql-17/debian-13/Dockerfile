# Copyright Reliza Incorporated. 2019 - 2025. Licensed under the terms of MIT.
# SPDX-License-Identifier: MIT

FROM docker.io/library/debian:13.3-slim@sha256:1d3c811171a08a5adaa4a163fbafd96b61b87aa871bbc7aa15431ac275d3d430

ARG EXTRA_LOCALES
ARG TARGETARCH
ARG WITH_ALL_LOCALES="no"
ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned



ENV HOME="/" \
    OS_ARCH="${TARGETARCH:-amd64}" \
    OS_FLAVOUR="debian-13" \
    OS_NAME="linux"

COPY prebuildfs /
SHELL ["/bin/bash", "-o", "errexit", "-o", "nounset", "-o", "pipefail", "-c"]

# Combined package installation and PostgreSQL setup to reduce layers
RUN apt-get update && install_packages ca-certificates=20250419 curl libbsd0 libbz2-1.0 libedit2 libffi8 libgcc-s1 libgmp10 libgnutls30 libhogweed6 libicu76 libidn2-0 libldap-common liblz4-1 liblzma5 libmd0 libnettle8 libp11-kit0 libpcre2-8-0=10.46-1~deb13u1 libreadline8t64 libsasl2-2 libsqlite3-0 libssl3t64 libstdc++6=14.2.0-19 libtasn1-6 libtinfo6 libunistring5 libuuid1 libxml2 libxslt1.1 libzstd1 locales procps zlib1g && \
    # Add PostgreSQL repository and install PostgreSQL
    apt-get update && apt-get install -y wget gnupg2 && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg && \
    echo 'deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt/ trixie-pgdg main' > /etc/apt/sources.list.d/pgdg.list
RUN apt-get update && \
    echo "=== Available PostgreSQL 17 versions ===" && \
    apt-cache madison postgresql-17 | head -10 && \
    apt-get install -y \
        postgresql-17=17.8-0+deb13u1 \
        postgresql-client-17=17.8-0+deb13u1 && \
    apt-mark hold postgresql-17 postgresql-client-17
    # Setup PostgreSQL directories and permissions
RUN mkdir -p /opt/relizaio/postgresql/{bin,share,conf,conf.default,logs,tmp} && \
    ln -sf /usr/lib/postgresql/17/bin/* /opt/relizaio/postgresql/bin/ && \
    cp /usr/share/postgresql/17/postgresql.conf.sample /opt/relizaio/postgresql/share/postgresql.conf.sample && \
    cp /usr/share/postgresql/17/postgresql.conf.sample /opt/relizaio/postgresql/conf/postgresql.conf && \
    usermod -u 1001 postgres && \
    groupmod -g 1001 postgres && \
    mkdir -p /var/run/postgresql && \
    chown -R postgres:postgres /opt/relizaio/postgresql /var/run/postgresql && \
    # Security and optimization
    chmod g+rwX /opt/relizaio && \
    find / -perm /6000 -type f -exec chmod a-s {} \; 2>/dev/null || true && \
    # Locale configuration (consolidated)
    echo 'en_US.UTF-8 UTF-8' >> /etc/locale.gen && \
    locale-gen && \
    echo 'LANG=C.UTF-8' > /etc/default/locale && \
    # Cleanup build-only packages and cache
    apt-get remove -y wget gnupg2 curl && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* /tmp/* /var/tmp/*

COPY rootfs /
RUN /opt/relizaio/scripts/postgresql/postunpack.sh && \
    /opt/relizaio/scripts/locales/generate-locales.sh

ENV APP_VERSION="17.8.0" \
    RELIZAIO_APP_NAME="postgresql" \
    IMAGE_REVISION="6" \
    LANG="en_US.UTF-8" \
    LANGUAGE="en_US:en" \
    NSS_WRAPPER_LIB="/opt/relizaio/common/lib/libnss_wrapper.so" \
    PATH="/opt/relizaio/postgresql/bin:$PATH"

VOLUME [ "/relizaio/postgresql", "/docker-entrypoint-initdb.d", "/docker-entrypoint-preinitdb.d" ]

EXPOSE 5432

USER 1001

LABEL org.opencontainers.image.base.name="docker.io/library/debian:13.3-slim" \
      org.opencontainers.image.description="PostgreSQL 17 packaged by Reliza Incorporated" \
      org.opencontainers.image.documentation="https://github.com/relizaio/dockerfile-collection/tree/main/postgresql-17/README.md" \
      org.opencontainers.image.source="https://github.com/relizaio/dockerfile-collection/tree/main/postgresql-17"
LABEL org.opencontainers.image.revision=$GIT_COMMIT
LABEL git_branch=$GIT_BRANCH
LABEL ci_environment=$CI_ENV
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.vendor="Reliza Incorporated"
LABEL org.opencontainers.image.title="ReARM PostgreSQL"
LABEL PG_VERSION="17.8-0+deb13u1"


ENTRYPOINT [ "/opt/relizaio/scripts/postgresql/entrypoint.sh" ]
CMD [ "/opt/relizaio/scripts/postgresql/run.sh" ]
