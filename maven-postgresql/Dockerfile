FROM ubuntu:focal-20210921 as build-stage
RUN apt-get update
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
RUN apt-get install --no-install-recommends -y tzdata
RUN dpkg-reconfigure --frontend noninteractive tzdata
RUN apt-get --no-install-recommends -y install openjdk-16-jdk maven postgresql-12