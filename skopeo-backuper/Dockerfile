FROM --platform=$BUILDPLATFORM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 AS kubectl-builder
WORKDIR /build
COPY . .
ARG TARGETOS
ARG TARGETARCH

ADD https://dl.k8s.io/v1.33.7/kubernetes-client-linux-${TARGETARCH}.tar.gz ./kubernetes-client-linux-${TARGETARCH}.tar.gz
RUN sha512sum -c tools.${TARGETARCH}.sha512
RUN tar -xzf kubernetes-client-linux-${TARGETARCH}.tar.gz && \
    mv kubernetes/client/bin/kubectl kubectl

FROM python:3.13.11-alpine3.23@sha256:2f607129b1b915a949320bf0c4831a73d1c1b1be663c2b1d8c93aa35a5f44a95 AS aws-builder

ARG AWS_CLI_VERSION=2.33.15
RUN apk add --no-cache git unzip groff build-base libffi-dev cmake
RUN git clone --single-branch --depth 1 -b ${AWS_CLI_VERSION} https://github.com/aws/aws-cli.git

WORKDIR /aws-cli
RUN ./configure --with-install-type=portable-exe --with-download-deps
RUN make
RUN make install

RUN find /usr/local/lib/aws-cli/awscli/data -name completions-1*.json -delete
RUN find /usr/local/lib/aws-cli/awscli/botocore/data -name examples-1.json -delete
RUN (cd /usr/local/lib/aws-cli; for a in *.so*; do test -f /lib/$a && rm $a; done) || true
RUN aws --version

FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 AS deliverable
COPY --from=aws-builder /usr/local/lib/aws-cli/ /usr/local/lib/aws-cli/
COPY --from=kubectl-builder /build/kubectl /usr/local/bin/kubectl
RUN ln -s /usr/local/lib/aws-cli/aws /usr/local/bin/aws

ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned
ENV AWSCLI_VERSION="2.33.15"
ENV KUBECTL_VERSION="1.33.7"

USER root

RUN adduser -u 1000 -D apprunner \
    && apk add --no-cache skopeo openssl ca-certificates \
    && skopeo --version \
    && aws --version \
    && kubectl version --client

COPY --chown=apprunner:apprunner ./entrypoint.sh /entrypoint.sh
RUN chmod 0770 ./entrypoint.sh

LABEL org.opencontainers.image.vendor="Reliza Incorporated"
LABEL org.opencontainers.image.title="Skopeo Backuper"
LABEL org.opencontainers.image.source="https://github.com/relizaio/dockerfile-collection"
LABEL org.opencontainers.image.license="MIT"
LABEL org.opencontainers.image.url="https://reliza.io"
LABEL org.opencontainers.image.base.name="registry.relizahub.com/library/skopeo-backuper"
LABEL org.opencontainers.image.revision=$GIT_COMMIT
LABEL git_branch=$GIT_BRANCH
LABEL ci_environment=$CI_ENV
LABEL org.opencontainers.image.version=$VERSION

USER apprunner

ENTRYPOINT ["/entrypoint.sh"]
