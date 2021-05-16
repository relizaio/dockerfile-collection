FROM alpine/helm:3.5.4 as build-stage

RUN apk add unzip

RUN wget https://d7ge14utcyki8.cloudfront.net/reliza-cli-download/2021.05.7/reliza-cli-2021.05.7-linux-amd64.zip

RUN echo 'a5682ee773c5f27019601188da8e023e3dfc8fbbc52fef3837e5df65e9057bbe  reliza-cli-2021.05.7-linux-amd64.zip' > shasum
RUN sha256sum -c shasum
RUN unzip reliza-cli-2021.05.7-linux-amd64.zip && cp reliza-cli /tmp/

FROM alpine/helm:3.5.4 as artifact-stage
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