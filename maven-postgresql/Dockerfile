FROM ubuntu:focal-20210921 as build-stage
RUN apt-get update
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
RUN apt-get install --no-install-recommends -y tzdata
RUN dpkg-reconfigure --frontend noninteractive tzdata
RUN apt-get --no-install-recommends -y install openjdk-16-jdk postgresql-12
ADD https://dlcdn.apache.org/maven/maven-3/3.8.3/binaries/apache-maven-3.8.3-bin.tar.gz /apache-maven-3.8.3-bin.tar.gz
COPY checksum.txt /
RUN sha512sum -c checksum.txt && tar xzvf apache-maven-3.8.3-bin.tar.gz
ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/apache-maven-3.8.3/bin"