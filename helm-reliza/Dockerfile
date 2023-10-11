FROM alpine/helm:3.12.1 as build-stage

RUN apk add unzip

RUN wget https://d7ge14utcyki8.cloudfront.net/reliza-cli-download/2023.09.3/reliza-cli-2023.09.3-linux-amd64.zip

RUN echo '1f56ce27a6929b9f0a3c16291225fed7f52d40cc82e9afee3f03e5b3fe230a07  reliza-cli-2023.09.3-linux-amd64.zip' > shasum
RUN sha256sum -c shasum
RUN unzip reliza-cli-2023.09.3-linux-amd64.zip && cp reliza-cli /tmp/

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
