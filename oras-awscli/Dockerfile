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

FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 AS oras-builder
ENV ORAS_VERSION="1.3.0"
COPY tools.sha256.sum .
RUN apk add curl && \
    curl -LO "https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_linux_amd64.tar.gz" \
    && sha256sum -c tools.sha256.sum \
    && mkdir -p oras-install/ \
    && tar -zxf oras_${ORAS_VERSION}_*.tar.gz -C oras-install/

# build the final image
FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 AS deliverable
COPY --from=aws-builder /usr/local/lib/aws-cli/ /usr/local/lib/aws-cli/
COPY --from=oras-builder oras-install/oras /usr/local/bin/oras
RUN ln -s /usr/local/lib/aws-cli/aws /usr/local/bin/aws

ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned
ENV AWSCLI_VERSION="2.33.15"
ENV ORAS_VERSION="1.3.0"

USER root

RUN adduser -u 1000 -D apprunner \
    && apk add openssl \
    && aws --version \
    && oras version

COPY --chown=apprunner:apprunner ./entrypoint.sh /entrypoint.sh
RUN chmod 0770 ./entrypoint.sh

LABEL org.opencontainers.image.vendor="Reliza Incorporated"
LABEL org.opencontainers.image.title="ORAS AWS CLI"
LABEL org.opencontainers.image.source="https://github.com/relizaio/dockerfile-collection"
LABEL org.opencontainers.image.license="MIT"
LABEL org.opencontainers.image.revision=$GIT_COMMIT
LABEL git_branch=$GIT_BRANCH
LABEL ci_environment=$CI_ENV
LABEL org.opencontainers.image.version=$VERSION

USER apprunner

ENTRYPOINT ["/entrypoint.sh"]