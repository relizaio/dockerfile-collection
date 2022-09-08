FROM bitnami/postgresql:14.5.0

ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned

USER root
RUN apt-get update
RUN apt-get install -y python3 python3-pip python3-setuptools
RUN pip3 install awscli --upgrade
RUN aws --version
RUN psql --version
COPY ./entrypoint.sh /entrypoint.sh
RUN chmod 0700 ./entrypoint.sh

LABEL git_commit $GIT_COMMIT
LABEL git_branch $GIT_BRANCH
LABEL ci_environment $CI_ENV
LABEL version $VERSION

ENTRYPOINT /entrypoint.sh