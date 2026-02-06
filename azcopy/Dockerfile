FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659
ARG TARGETOS
ARG TARGETARCH 
ARG AZVERSION=10.32.0
ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG VERSION=not_versioned

RUN apk --update add --virtual build-dependencies --no-cache wget tar 
RUN apk --update add libc6-compat ca-certificates
RUN apk add --no-cache postgresql-client

RUN wget -O azcopy.tar.gz https://github.com/Azure/azure-storage-azcopy/releases/download/v${AZVERSION}/azcopy_${TARGETOS}_${TARGETARCH}_${AZVERSION}.tar.gz && \
    tar -xf azcopy.tar.gz && ls && \
    mv azcopy_${TARGETOS}_${TARGETARCH}_${AZVERSION}/azcopy /usr/local/bin && \
    rm -rf azcopy* && \
    apk del build-dependencies

LABEL git_commit $GIT_COMMIT
LABEL git_branch $GIT_BRANCH
LABEL ci_environment $CI_ENV
LABEL version $VERSION

ENTRYPOINT ["/usr/local/bin/azcopy"]