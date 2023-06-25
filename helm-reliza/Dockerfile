FROM alpine/helm:3.12.1 as build-stage

RUN apk add unzip

RUN wget https://d7ge14utcyki8.cloudfront.net/reliza-cli-download/2023.05.11/reliza-cli-2023.05.11-linux-amd64.zip

RUN echo '8cb995a4737ebfd86592c598f5c23ee54fd234be1d8a5cec84a76e9aaf2d87cb  reliza-cli-2023.05.11-linux-amd64.zip' > shasum
RUN sha256sum -c shasum
RUN unzip reliza-cli-2023.05.11-linux-amd64.zip && cp reliza-cli /tmp/

FROM alpine/helm:3.12.1 as artifact-stage
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