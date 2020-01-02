FROM debian:stable-20191224 as build-stage
RUN apt-get update
RUN apt-get --no-install-recommends -y install openjdk-11-jdk maven
RUN mkdir /workdir
WORKDIR /workdir