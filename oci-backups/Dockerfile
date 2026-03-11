FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 AS oras-builder
ENV ORAS_VERSION="1.3.0"
WORKDIR /tools
COPY tools.sha256.sum .

RUN apk add --no-cache curl && \
    curl -LO "https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_linux_amd64.tar.gz" \
    && sha256sum -c tools.sha256.sum \
    && mkdir -p oras-install/ \
    && tar -zxf oras_${ORAS_VERSION}_*.tar.gz -C oras-install/

FROM golang:1.25.6-alpine3.23@sha256:98e6cffc31ccc44c7c15d83df1d69891efee8115a5bb7ede2bf30a38af3e3c92 AS go-builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o oci-backup .

FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 AS deliverable

RUN apk add --no-cache ca-certificates tzdata \
    && adduser -u 1000 -D apprunner

COPY --from=oras-builder /tools/oras-install/oras /usr/local/bin/oras
COPY --from=go-builder /build/oci-backup /usr/local/bin/oci-backup

RUN chmod +x /usr/local/bin/oras /usr/local/bin/oci-backup \
    && oras version

ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned
ENV ORAS_VERSION="1.3.0"

LABEL org.opencontainers.image.vendor="Reliza Incorporated"
LABEL org.opencontainers.image.title="OCI Backup Tool"
LABEL org.opencontainers.image.description="OCI registry backup tool with S3 and Azure storage support"
LABEL org.opencontainers.image.source="https://github.com/relizaio/dockerfile-collection"
LABEL org.opencontainers.image.license="MIT"
LABEL org.opencontainers.image.url="https://reliza.io"
LABEL org.opencontainers.image.base.name="registry.relizahub.com/library/oras-awscli"
LABEL org.opencontainers.image.revision=$GIT_COMMIT
LABEL git_branch=$GIT_BRANCH
LABEL ci_environment=$CI_ENV
LABEL org.opencontainers.image.version=$VERSION

USER apprunner

ENTRYPOINT ["/usr/local/bin/oci-backup"]