FROM --platform=$BUILDPLATFORM alpine:3.17.0@sha256:8914eb54f968791faf6a8638949e480fef81e697984fba772b3976835194c6d4 as tools-stage
ARG TARGETARCH
WORKDIR /tools
RUN apk add --update --no-cache unzip
COPY checksum.${TARGETARCH}.txt .
ADD https://storage.googleapis.com/kubernetes-release/release/v1.28.4/bin/linux/${TARGETARCH}/kubectl ./kubectl-${TARGETARCH}
ADD https://d7ge14utcyki8.cloudfront.net/reliza-cli-download/2023.09.3/reliza-cli-2023.09.3-linux-${TARGETARCH}.zip ./reliza-cli-2023.09.3-linux-${TARGETARCH}.zip
RUN sha256sum -c checksum.${TARGETARCH}.txt
RUN unzip reliza-cli-2023.09.3-linux-${TARGETARCH}.zip
RUN mv kubectl-${TARGETARCH} kubectl

FROM alpine:3.17.0@sha256:8914eb54f968791faf6a8638949e480fef81e697984fba772b3976835194c6d4 as release-stage
ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned
ARG TARGETARCH
RUN apk add --no-cache bash jq
RUN mkdir /app
RUN adduser -u 1000 -D apprunner && chown apprunner:apprunner -R /app
RUN mkdir /app/resources && chown apprunner:apprunner /app/resources
COPY --from=tools-stage --chown=apprunner:apprunner /tools/kubectl /app/kubectl
COPY --from=tools-stage --chown=apprunner:apprunner /tools/reliza-cli /app/reliza-cli
COPY --chown=apprunner:apprunner entrypoint.sh /app/entrypoint.sh
RUN chmod 0500 /app/entrypoint.sh && chmod 0700 /app/kubectl
ENV HUB_URI https://app.relizahub.com
ENV NAMESPACE allnamespaces
ENV SENDER_ID default

USER apprunner
RUN echo "{}" > /app/resources/images
RUN echo "version=$VERSION" > /app/version && echo "commit=$GIT_COMMIT" >> /app/version && echo "branch=$GIT_BRANCH" >> /app/version
WORKDIR app
LABEL git_commit $GIT_COMMIT
LABEL git_branch $GIT_BRANCH
LABEL ci_environment $CI_ENV
LABEL version $VERSION
ENTRYPOINT ["./entrypoint.sh"]
