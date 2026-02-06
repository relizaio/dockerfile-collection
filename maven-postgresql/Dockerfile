FROM debian:13.3-slim@sha256:f6e2cfac5cf956ea044b4bd75e6397b4372ad88fe00908045e9a0d21712ae3ba AS build-stage
ADD https://dlcdn.apache.org/maven/maven-3/3.9.12/binaries/apache-maven-3.9.12-bin.tar.gz /tmp/apache-maven-3.9.12-bin.tar.gz
COPY checksum.txt /tmp/
WORKDIR /tmp
RUN sha512sum -c checksum.txt && tar xzvf apache-maven-3.9.12-bin.tar.gz

FROM debian:13.3-slim@sha256:f6e2cfac5cf956ea044b4bd75e6397b4372ad88fe00908045e9a0d21712ae3ba AS package-stage
COPY --from=build-stage  /tmp/apache-maven-3.9.12 /apache-maven-3.9.12
RUN apt update
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
RUN apt-get install --no-install-recommends -y tzdata
RUN dpkg-reconfigure --frontend noninteractive tzdata
RUN apt --no-install-recommends -y install openjdk-21-jdk postgresql-17
ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/apache-maven-3.9.12/bin"