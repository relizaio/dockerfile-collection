FROM busybox:1.34.1@sha256:139abcf41943b8bcd4bc5c42ee71ddc9402c7ad69ad9e177b0a9bc4541f14924
ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned
COPY checksum.txt /
ADD https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 /bin/jq
RUN sha256sum -c checksum.txt
RUN chmod 0700 /bin/jq
LABEL git_commit $GIT_COMMIT
LABEL git_branch $GIT_BRANCH
LABEL ci_environment $CI_ENV
LABEL version $VERSION
ENTRYPOINT ["/bin/jq"]