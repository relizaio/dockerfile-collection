FROM --platform=$BUILDPLATFORM relizaio/reliza-cli:2023.09.7@sha256:12300d578c6f0c3d1cf34ebee58d7b3e1e89c362ab15283fe5dab4661950cf55
ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned
ARG TARGETARCH
USER root
RUN apk add bash jq
RUN mkdir /resources && chown apprunner:apprunner /resources
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
