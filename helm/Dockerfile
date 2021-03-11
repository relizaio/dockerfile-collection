FROM alpine/helm:3.5.2

RUN apk add git

RUN helm plugin install https://github.com/chartmuseum/helm-push.git

ENV HELM_EXPERIMENTAL_OCI=1

ENTRYPOINT ["helm"]
CMD ["--help"]