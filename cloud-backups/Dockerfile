# oras is compiled here rather than downloaded as an upstream release tarball.
# The published release binaries are built with an older Go toolchain whose
# stdlib carries advisories a current toolchain clears, and oras v1.3.4 (the
# latest release) still pins golang.org/x/crypto v0.52.0, which carries
# GO-2026-6303 / GO-2026-6354 / GO-2026-6355 (fixed in v0.55.0 / v0.56.0).
# `go install pkg@version` cannot override a dependency of the installed module,
# so oras is built through a throwaway wrapper module: it requires the tagged
# oras release as a tool and raises x/crypto by minimum version selection. The
# oras source is the unmodified v1.3.4 module (checked against sum.golang.org),
# and the binary's build info records oras.land/oras v1.3.4 plus the raised
# x/crypto, so the SBOM still names the exact oras release.
FROM golang:1.26.8-alpine3.24@sha256:8ac98ca534ac3f51e1f420a1dd2c15e74c75cfa0f23f3ad27eb5d7236c349a0c AS oras-builder
ENV ORAS_VERSION="1.3.4"
ENV XCRYPTO_VERSION="0.57.0"
ENV CGO_ENABLED=0
WORKDIR /tools/oras-build
RUN go mod init oras-build \
    && go get -tool oras.land/oras/cmd/oras@v${ORAS_VERSION} \
    && go get golang.org/x/crypto@v${XCRYPTO_VERSION} \
    && mkdir -p /tools/oras-install \
    && go build -trimpath -ldflags="-s -w" -o /tools/oras-install/oras oras.land/oras/cmd/oras \
    && go version -m /tools/oras-install/oras | grep -q "oras.land/oras[[:space:]]*v${ORAS_VERSION}[[:space:]]" \
    && go version -m /tools/oras-install/oras | grep -q "golang.org/x/crypto[[:space:]]*v${XCRYPTO_VERSION}[[:space:]]"

FROM golang:1.26.8-alpine3.24@sha256:8ac98ca534ac3f51e1f420a1dd2c15e74c75cfa0f23f3ad27eb5d7236c349a0c AS go-builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o cloud-backup .

# The runtime filesystem is assembled on Alpine and then flattened into a single
# layer. `apk upgrade` pulls fixed packages that are newer than the ones baked
# into the alpine base layer (e.g. libcrypto3/libssl3 3.5.8-r0 -> 3.5.9-r0, zlib
# 1.3.2-r0 -> 1.3.2-r1), but in a layered image the superseded packages still
# ship in the base layer, and an all-layers SBOM (which CI produces) reports
# them. Copying the finished rootfs onto scratch means only the upgraded
# packages ship; the apk database is kept, so scanners still see every package.
FROM alpine:3.24.2@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6 AS rootfs

RUN apk upgrade --no-cache \
    && apk add --no-cache ca-certificates tzdata postgresql17-client \
    && adduser -u 1000 -D apprunner

COPY --from=oras-builder /tools/oras-install/oras /usr/local/bin/oras
COPY --from=go-builder /build/cloud-backup /usr/local/bin/cloud-backup

RUN chmod +x /usr/local/bin/oras /usr/local/bin/cloud-backup \
    && oras version \
    && pg_dump --version

FROM scratch AS deliverable
COPY --from=rootfs / /
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned

LABEL org.opencontainers.image.vendor="Reliza Incorporated"
LABEL org.opencontainers.image.title="Cloud Backup Tool"
LABEL org.opencontainers.image.description="Unified backup tool for OCI registries and PostgreSQL databases with S3 and Azure storage support"
LABEL org.opencontainers.image.source="https://github.com/relizaio/dockerfile-collection"
LABEL org.opencontainers.image.license="MIT"
LABEL org.opencontainers.image.url="https://reliza.io"
LABEL org.opencontainers.image.base.name="registry.relizahub.com/library/cloud-backup"
LABEL org.opencontainers.image.revision=$GIT_COMMIT
LABEL git_branch=$GIT_BRANCH
LABEL ci_environment=$CI_ENV
LABEL org.opencontainers.image.version=$VERSION

USER apprunner

ENTRYPOINT ["/usr/local/bin/cloud-backup"]