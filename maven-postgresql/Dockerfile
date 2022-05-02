FROM ubuntu:jammy as build-stage
ADD https://dlcdn.apache.org/maven/maven-3/3.8.5/binaries/apache-maven-3.8.5-bin.tar.gz /tmp/apache-maven-3.8.5-bin.tar.gz
COPY maven-postgresql/checksum.txt /tmp/
WORKDIR /tmp
RUN sha512sum -c checksum.txt && tar xzvf apache-maven-3.8.5-bin.tar.gz

FROM ubuntu:jammy as package-stage
COPY --from=build-stage  /tmp/apache-maven-3.8.5 /apache-maven-3.8.5
RUN apt-get update
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
RUN apt-get install --no-install-recommends -y tzdata
RUN dpkg-reconfigure --frontend noninteractive tzdata
RUN apt-get --no-install-recommends -y install openjdk-17-jdk postgresql-14
ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/apache-maven-3.8.5/bin"