FROM python:3.11-alpine3.19 as builder

ARG AWS_CLI_VERSION=2.15.30
RUN apk add --no-cache git unzip groff build-base libffi-dev cmake
RUN git clone --single-branch --depth 1 -b ${AWS_CLI_VERSION} https://github.com/aws/aws-cli.git

WORKDIR aws-cli
RUN ./configure --with-install-type=portable-exe --with-download-deps
RUN make
RUN make install

# reduce image size: remove autocomplete and examples
RUN rm -rf \
    /usr/local/lib/aws-cli/aws_completer \
    /usr/local/lib/aws-cli/awscli/data/ac.index \
    /usr/local/lib/aws-cli/awscli/examples
RUN find /usr/local/lib/aws-cli/awscli/data -name completions-1*.json -delete
RUN find /usr/local/lib/aws-cli/awscli/botocore/data -name examples-1.json -delete
RUN (cd /usr/local/lib/aws-cli; for a in *.so*; do test -f /lib/$a && rm $a; done)
RUN aws --version

# build the final image
FROM alpine:3.19.1
COPY --from=builder /usr/local/lib/aws-cli/ /usr/local/lib/aws-cli/
RUN ln -s /usr/local/lib/aws-cli/aws /usr/local/bin/aws

ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned
ENV AWSCLI_VERSION "2.15.30"

USER root

RUN apk add postgresql-client

RUN aws --version
RUN psql --version
COPY ./entrypoint.sh /entrypoint.sh
RUN chmod 0770 ./entrypoint.sh

LABEL git_commit $GIT_COMMIT
LABEL git_branch $GIT_BRANCH
LABEL ci_environment $CI_ENV
LABEL version $VERSION

ENTRYPOINT /entrypoint.sh