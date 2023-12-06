From alpine:3.15.4@sha256:a777c9c66ba177ccfea23f2a216ff6721e78a662cd17019488c417135299cd89
ARG TARGETOS
ARG TARGETARCH 
ARG AZVERSION=azcopy_${TARGETOS}_${TARGETARCH}_10.15.0
ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned

RUN apk --update add --virtual build-dependencies --no-cache wget tar 
RUN apk --update add libc6-compat ca-certificates
RUN apk add --no-cache postgresql-client

RUN wget -O azcopy.tar.gz https://azcopyvnext.azureedge.net/release20220511/${AZVERSION}.tar.gz && \
    tar -xf azcopy.tar.gz && \
    mv ${AZVERSION}/azcopy /usr/local/bin && \
    rm -rf azcopy* && \
    apk del build-dependencies

LABEL git_commit $GIT_COMMIT
LABEL git_branch $GIT_BRANCH
LABEL ci_environment $CI_ENV
LABEL version $VERSION

ENTRYPOINT ["/usr/local/bin/azcopy"]