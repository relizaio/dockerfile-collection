FROM --platform=$BUILDPLATFORM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 AS build-stage
WORKDIR /build
COPY . .
ARG TARGETOS
ARG TARGETARCH

ADD https://dl.k8s.io/v1.32.9/kubernetes-client-linux-${TARGETARCH}.tar.gz ./kubernetes-client-${TARGETARCH}.tar.gz
RUN sha512sum -c tools.${TARGETARCH}.sha512
RUN tar -xzf kubernetes-client-${TARGETARCH}.tar.gz && \
    mv kubernetes/client/bin/kubectl kubectl && \
    rm -rf kubernetes kubernetes-client-${TARGETARCH}.tar.gz

FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 AS release-stage
ARG TARGETARCH
ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned
ENV KUBECTL_VERSION=1.32.9

USER root
RUN apk add --no-cache ca-certificates && \
    mkdir /app && \
    adduser -u 1000 -D apprunner && \
    chown apprunner:apprunner -R /app
COPY --from=build-stage --chown=apprunner:apprunner /build/kubectl /app/kubectl
COPY --chown=apprunner:apprunner entrypoint.sh /app/entrypoint.sh
RUN chmod 0500 /app/kubectl && chmod 0555 /app/entrypoint.sh && \
    /app/kubectl version --client

# Add kubectl to PATH so it can be called directly from shell commands
ENV PATH="/app:${PATH}"

USER apprunner
RUN echo "version=$VERSION" > /app/version && echo "commit=$GIT_COMMIT" >> /app/version && echo "branch=$GIT_BRANCH" >> /app/version

LABEL org.opencontainers.image.revision=$GIT_COMMIT
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.title="kubectl"
LABEL org.opencontainers.image.description="Kubectl ${KUBECTL_VERSION} with shell-compatible wrapper for Kubernetes operations"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL git_branch=$GIT_BRANCH
LABEL ci_environment=$CI_ENV
LABEL kubectl_version=$KUBECTL_VERSION

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["version", "--client"]