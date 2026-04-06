FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 AS oras-builder
ENV ORAS_VERSION="1.3.1"
WORKDIR /tools
COPY tools.sha256.sum .

RUN apk add --no-cache curl && \
    curl -LO "https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_linux_amd64.tar.gz" \
    && sha256sum -c tools.sha256.sum \
    && mkdir -p oras-install/ \
    && tar -zxf oras_${ORAS_VERSION}_*.tar.gz -C oras-install/

FROM golang:1.26.1-alpine3.23@sha256:2389ebfa5b7f43eeafbd6be0c3700cc46690ef842ad962f6c5bd6be49ed82039 AS go-builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o cloud-backup .

FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 AS deliverable

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
ENV ORAS_VERSION="1.3.0"

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