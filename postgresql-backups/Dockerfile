FROM golang:1.25.6-alpine3.23@sha256:98e6cffc31ccc44c7c15d83df1d69891efee8115a5bb7ede2bf30a38af3e3c92 AS go-builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o pg-backup .

FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 AS deliverable

RUN apk add --no-cache ca-certificates tzdata postgresql17-client \
    && adduser -u 1000 -D apprunner

COPY --from=go-builder /build/pg-backup /usr/local/bin/pg-backup

RUN chmod +x /usr/local/bin/pg-backup

ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned

LABEL org.opencontainers.image.vendor="Reliza Incorporated"
LABEL org.opencontainers.image.title="PostgreSQL Backup Tool"
LABEL org.opencontainers.image.description="PostgreSQL backup tool with S3 and Azure storage support and age encryption"
LABEL org.opencontainers.image.source="https://github.com/relizaio/dockerfile-collection"
LABEL org.opencontainers.image.license="MIT"
LABEL org.opencontainers.image.url="https://reliza.io"
LABEL org.opencontainers.image.base.name="registry.relizahub.com/library/postgresql-backup"
LABEL org.opencontainers.image.revision=$GIT_COMMIT
LABEL git_branch=$GIT_BRANCH
LABEL ci_environment=$CI_ENV
LABEL org.opencontainers.image.version=$VERSION

USER apprunner

ENTRYPOINT ["/usr/local/bin/pg-backup"]
