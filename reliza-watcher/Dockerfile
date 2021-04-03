FROM relizaio/reliza-cli@sha256:f4ce873ff0b939cf03a77f407208ee8d622c649a69972401e22ba216a179b01f
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
ENV HUB_URI https://relizahub.com
ENV NAMESPACE allnamespaces
ENV SENDER_ID default
ADD https://storage.googleapis.com/kubernetes-release/release/v1.18.3/bin/linux/amd64/kubectl /bin/kubectl
RUN sha256sum -c checksum.txt
RUN chmod 0755 /bin/kubectl
USER apprunner
RUN echo "{}" > /resources/images
LABEL git_commit $GIT_COMMIT
LABEL git_branch $GIT_BRANCH
LABEL ci_environment $CI_ENV
LABEL version $VERSION
ENTRYPOINT ["/entrypoint.sh"]