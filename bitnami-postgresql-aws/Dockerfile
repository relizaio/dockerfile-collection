FROM bitnami/postgresql:12.2.0-ol-7-r6@sha256:7d67254072ba69b05b68ac150e18f6de53929f8e0f196723ea001d187adbaeda
USER root
RUN yum update
RUN yum install -y python3 python3-pip python3-setuptools
RUN pip3 install awscli --upgrade
RUN aws --version
RUN psql --version