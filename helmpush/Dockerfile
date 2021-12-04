FROM alpine/helm:3.7.1

ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned

RUN apk add git

RUN helm plugin install https://github.com/chartmuseum/helm-push.git

ENV HELM_EXPERIMENTAL_OCI=1

LABEL git_commit $GIT_COMMIT
LABEL git_branch $GIT_BRANCH
LABEL ci_environment $CI_ENV
LABEL version $VERSION

ENTRYPOINT ["helm"]
CMD ["--help"]