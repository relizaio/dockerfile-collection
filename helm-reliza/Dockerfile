FROM --platform=$BUILDPLATFORM alpine/helm:3.13.2 as build-stage

RUN apk add unzip

ARG TARGETOS
ARG TARGETARCH 

RUN wget https://d7ge14utcyki8.cloudfront.net/reliza-cli-download/2023.09.3/reliza-cli-2023.09.3-${TARGETOS}-${TARGETARCH}.zip

COPY . .
RUN sha256sum -c shasum-${TARGETARCH}
RUN unzip reliza-cli-2023.09.3-${TARGETOS}-${TARGETARCH}.zip && cp reliza-cli /tmp/

FROM alpine/helm:3.13.2 as artifact-stage
ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned
RUN apk add jq
COPY --from=build-stage /tmp/reliza-cli /bin/
RUN adduser -D apprunner && chown apprunner:apprunner /apps
LABEL git_commit $GIT_COMMIT
LABEL git_branch $GIT_BRANCH
LABEL ci_environment $CI_ENV
LABEL version $VERSION
USER apprunner
