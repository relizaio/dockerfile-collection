FROM alpine/helm:3.7.2 as build-stage

RUN apk add unzip

RUN wget https://d7ge14utcyki8.cloudfront.net/reliza-cli-download/2022.07.2/reliza-cli-2022.07.2-linux-amd64.zip

RUN echo '3cffc99e639d7cd6b5f4dcd2e59a4a9d791b56c4ac0a42329e42260d6c86d35b  reliza-cli-2022.07.2-linux-amd64.zip' > shasum
RUN sha256sum -c shasum
RUN unzip reliza-cli-2022.07.2-linux-amd64.zip && cp reliza-cli /tmp/

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