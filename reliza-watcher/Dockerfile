FROM relizaio/reliza-cli:2023.09.7@sha256:2998dd9a86485a204b834a589a821fe05130e9861c3381c39351d99775accf45
ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned
USER root
RUN apk add bash jq
RUN mkdir /resources && chown apprunner:apprunner /resources
ARG TARGETARCH
COPY checksum.${TARGETARCH}.txt /
COPY --chown=apprunner:apprunner entrypoint.sh /
RUN chmod 0500 /entrypoint.sh
ENV HUB_URI https://app.relizahub.com
ENV NAMESPACE allnamespaces
ENV SENDER_ID default
ADD https://storage.googleapis.com/kubernetes-release/release/v1.28.4/bin/linux/${TARGETARCH}/kubectl ./kubectl-${TARGETARCH}
RUN sha256sum -c checksum.${TARGETARCH}.txt
RUN mv kubectl-${TARGETARCH} /bin/kubectl && chmod 0755 /bin/kubectl
USER apprunner
RUN echo "{}" > /resources/images
LABEL git_commit $GIT_COMMIT
LABEL git_branch $GIT_BRANCH
LABEL ci_environment $CI_ENV
LABEL version $VERSION
ENTRYPOINT ["/entrypoint.sh"]
