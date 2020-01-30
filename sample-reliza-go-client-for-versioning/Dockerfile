FROM relizaio/reliza-go-client as version-stage
ARG MODIFIER=Snapshot
ARG CI_ENV=noci
ARG GIT_COMMIT=git_commit_undefined
ARG GIT_BRANCH=git_branch_undefined
ARG RELIZA_VERSION_ID=noid
ARG RELIZA_VERSION_KEY=nokey
ARG USE_VERSIONING=false
RUN if [ "$USE_VERSIONING" = "true" ]; then echo "version=$(/app/app getversion -u https://test.relizahub.com -k $RELIZA_VERSION_KEY -i $RELIZA_VERSION_ID -b master --modifier $MODIFIER --metadata $CI_ENV | grep version | cut -d ':' -f 2 | cut -d '"' -f 2)" > /tmp/version; else echo "version=not_set" >> /tmp/version; fi 
RUN echo "commit=$GIT_COMMIT" >> /tmp/version && echo "branch=$GIT_BRANCH" >> /tmp/version