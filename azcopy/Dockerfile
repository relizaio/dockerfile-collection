From alpine:3.15.4@sha256:a777c9c66ba177ccfea23f2a216ff6721e78a662cd17019488c417135299cd89

ARG VERSION=azcopy_linux_amd64_10.15.0

RUN apk --update add --virtual build-dependencies --no-cache wget tar 
RUN apk --update add libc6-compat ca-certificates

RUN wget -O azcopy.tar.gz https://azcopyvnext.azureedge.net/release20220511/${VERSION}.tar.gz && \
    tar -xf azcopy.tar.gz && \
    mv ${VERSION}/azcopy /usr/local/bin && \
    rm -rf azcopy* && \
    apk del build-dependencies

ENTRYPOINT ["/usr/local/bin/azcopy"]