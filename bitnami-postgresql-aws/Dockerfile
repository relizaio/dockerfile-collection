FROM bitnami/postgresql:14
USER root
RUN yum update
RUN yum install -y python3 python3-pip python3-setuptools
RUN pip3 install awscli --upgrade
RUN aws --version
RUN psql --version
COPY ./entrypoint.sh /entrypoint.sh
RUN chmod 0700 ./entrypoint.sh
ENTRYPOINT /entrypoint.sh