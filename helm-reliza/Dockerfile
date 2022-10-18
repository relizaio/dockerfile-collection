FROM alpine/helm:3.7.2 as build-stage

RUN apk add unzip

RUN wget https://d7ge14utcyki8.cloudfront.net/reliza-cli-download/2022.07.6/reliza-cli-2022.07.6-linux-amd64.zip

RUN echo 'b91ee2d2e77993d04e3190269bff92528e86df75524fd6b3b8acd81f30c14677  reliza-cli-2022.07.6-linux-amd64.zip' > shasum
RUN sha256sum -c shasum
RUN unzip reliza-cli-2022.07.6-linux-amd64.zip && cp reliza-cli /tmp/

FROM alpine/helm:3.7.2 as artifact-stage
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