# oras is compiled here rather than downloaded as an upstream release tarball.
# The published v1.3.3 binary is built with Go 1.25.7, whose stdlib carries 29
# advisories including CVE-2026-39821 (critical) -- and 1.3.3 is the latest
# release, so no version bump clears them. Building the same tagged source with
# a current toolchain does. The version is still pinned, and the module proxy +
# go.sum give provenance in place of the tarball checksum.
FROM golang:1.26.6-alpine3.24@sha256:3889b425f035be855a72fb4755265311293b6d414521f0a519d819df32222d83 AS oras-builder
ENV ORAS_VERSION="1.3.3"
WORKDIR /tools
ENV CGO_ENABLED=0
RUN go install -trimpath -ldflags="-s -w" oras.land/oras/cmd/oras@v${ORAS_VERSION} \
    && mkdir -p oras-install/ \
    && cp "$(go env GOPATH)/bin/oras" oras-install/oras

FROM golang:1.26.6-alpine3.24@sha256:3889b425f035be855a72fb4755265311293b6d414521f0a519d819df32222d83 AS go-builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o cloud-backup .

FROM alpine:3.24.1@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b AS deliverable

RUN apk add --no-cache ca-certificates tzdata postgresql17-client \
    && adduser -u 1000 -D apprunner

COPY --from=oras-builder /tools/oras-install/oras /usr/local/bin/oras
COPY --from=go-builder /build/cloud-backup /usr/local/bin/cloud-backup

RUN chmod +x /usr/local/bin/oras /usr/local/bin/cloud-backup \
    && oras version \
    && pg_dump --version

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