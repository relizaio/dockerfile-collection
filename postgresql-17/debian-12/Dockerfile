# Copyright Reliza Incorporated. 2019 - 2025. Licensed under the terms of AGPL-3.0-only.
# SPDX-License-Identifier: AGPL-3.0-only

FROM docker.io/library/debian:12.12-slim@sha256:df52e55e3361a81ac1bead266f3373ee55d29aa50cf0975d440c2be3483d8ed3

ARG EXTRA_LOCALES
ARG TARGETARCH
ARG WITH_ALL_LOCALES="no"

LABEL org.opencontainers.image.base.name="docker.io/library/debian:12.12-slim" \
      org.opencontainers.image.created="2025-09-07T02:20:20Z" \
      org.opencontainers.image.description="Application packaged by Reliza Incorporated" \
      org.opencontainers.image.documentation="https://github.com/relizaio/dockerfile-collection/tree/main/postgresql-17/README.md" \
      org.opencontainers.image.source="https://github.com/relizaio/dockerfile-collection/tree/main/postgresql-17" \
      org.opencontainers.image.title="postgresql" \
      org.opencontainers.image.vendor="Reliza Incorporated" \
      org.opencontainers.image.version="17.6.0"

ENV HOME="/" \
    OS_ARCH="${TARGETARCH:-amd64}" \
    OS_FLAVOUR="debian-12" \
    OS_NAME="linux"

COPY prebuildfs /
SHELL ["/bin/bash", "-o", "errexit", "-o", "nounset", "-o", "pipefail", "-c"]

# Combined package installation and PostgreSQL setup to reduce layers
RUN install_packages ca-certificates curl libbsd0 libbz2-1.0 libedit2 libffi8 libgcc-s1 libgmp10 libgnutls30 libhogweed6 libicu72 libidn2-0 libldap-2.5-0 liblz4-1 liblzma5 libmd0 libnettle8 libp11-kit0 libpcre3 libreadline8 libsasl2-2 libsqlite3-0 libssl3 libstdc++6 libtasn1-6 libtinfo6 libunistring2 libuuid1 libxml2 libxslt1.1 libzstd1 locales procps zlib1g && \
    # Add PostgreSQL repository and install PostgreSQL
    apt-get update && apt-get install -y wget gnupg2 && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg && \
    echo 'deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt/ bookworm-pgdg main' > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && \
    apt-get install -y \
        postgresql-17=17.6-1.pgdg12+1 \
        postgresql-client-17=17.6-1.pgdg12+1 && \
    apt-mark hold postgresql-17 postgresql-client-17 && \
    # Setup PostgreSQL directories and permissions
    mkdir -p /opt/relizaio/postgresql/{bin,share,conf,conf.default,logs,tmp} && \
    ln -sf /usr/lib/postgresql/17/bin/* /opt/relizaio/postgresql/bin/ && \
    cp /usr/share/postgresql/17/postgresql.conf.sample /opt/relizaio/postgresql/share/postgresql.conf.sample && \
    usermod -u 1001 postgres && \
    groupmod -g 1001 postgres && \
    mkdir -p /var/run/postgresql && \
    chown -R postgres:postgres /opt/relizaio/postgresql /var/run/postgresql && \
    # Security and optimization
    chmod g+rwX /opt/relizaio && \
    find / -perm /6000 -type f -exec chmod a-s {} \; 2>/dev/null || true && \
    # Locale configuration (consolidated)
    update-locale LANG=C.UTF-8 LC_MESSAGES=POSIX && \
    DEBIAN_FRONTEND=noninteractive dpkg-reconfigure locales && \
    echo 'en_US.UTF-8 UTF-8' >> /etc/locale.gen && \
    # Cleanup build-only packages and cache
    apt-get remove -y wget gnupg2 curl && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* /tmp/* /var/tmp/*

COPY rootfs /
RUN /opt/relizaio/scripts/postgresql/postunpack.sh && \
    /opt/relizaio/scripts/locales/generate-locales.sh

ENV APP_VERSION="17.6.0" \
    RELIZAIO_APP_NAME="postgresql" \
    IMAGE_REVISION="6" \
    LANG="en_US.UTF-8" \
    LANGUAGE="en_US:en" \
    NSS_WRAPPER_LIB="/opt/relizaio/common/lib/libnss_wrapper.so" \
    PATH="/opt/relizaio/postgresql/bin:$PATH"

VOLUME [ "/relizaio/postgresql", "/docker-entrypoint-initdb.d", "/docker-entrypoint-preinitdb.d" ]

EXPOSE 5432

USER 1001
ENTRYPOINT [ "/opt/relizaio/scripts/postgresql/entrypoint.sh" ]
CMD [ "/opt/relizaio/scripts/postgresql/run.sh" ]
