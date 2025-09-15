# Bitnami package for Sealed Secrets

## What is Sealed Secrets?

> Sealed Secrets are "one-way" encrypted K8s Secrets that can be created by anyone, but can only be decrypted by the controller running in the target cluster recovering the original object.

[Overview of Sealed Secrets](https://github.com/bitnami-labs/sealed-secrets)

## TL;DR

```console
docker run --name sealed-secrets bitnami/sealed-secrets:latest
```

### Running commands

To run commands inside this container you can use `docker run`, for example to execute `kubeseal --version` you can follow the example below:

```console
docker run --rm --name sealed-secrets bitnami/sealed-secrets:latest -- kubeseal --version
```

