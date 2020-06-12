FROM python:3.8.3-alpine3.12@sha256:c5623df482648cacece4f9652a0ae04b51576c93773ccd43ad459e2a195906dd
ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned
ENV PATH="$PATH:/root/.local/bin"
RUN pip install --user --upgrade awscli
LABEL git_commit $GIT_COMMIT
LABEL git_branch $GIT_BRANCH
LABEL ci_environment $CI_ENV
LABEL version $VERSION
ENTRYPOINT ["aws"]