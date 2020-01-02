FROM debian:stable-20191224
RUN apt-get update
RUN apt-get --no-install-recommends -y install python3 python3-pip
RUN apt-get --no-install-recommends -y install python3-setuptools
RUN pip3 install awscli --upgrade
RUN aws --version