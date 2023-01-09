FROM relizaio/reliza-cli:2023.01.5@sha256:17f6c3ce22238d1b52ca35df127a92406847674a1d8da4642f1bd91e99a176ec
ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned
USER root
RUN apk add bash jq
RUN mkdir /resources && chown apprunner:apprunner /resources
COPY checksum.txt /
COPY --chown=apprunner:apprunner entrypoint.sh /
RUN chmod 0500 /entrypoint.sh
ENV HUB_URI https://app.relizahub.com
ENV NAMESPACE allnamespaces
ENV SENDER_ID default
ADD https://storage.googleapis.com/kubernetes-release/release/v1.23.14/bin/linux/amd64/kubectl /bin/kubectl
RUN sha256sum -c checksum.txt
RUN chmod 0755 /bin/kubectl
USER apprunner
RUN echo "{}" > /resources/images
LABEL git_commit $GIT_COMMIT
LABEL git_branch $GIT_BRANCH
LABEL ci_environment $CI_ENV
LABEL version $VERSION
ENTRYPOINT ["/entrypoint.sh"]