FROM openjdk:8-jdk
WORKDIR /opt

ENV SCALA_VERSION=2.13.8
ENV SBT_VERSION=1.8.0

RUN apt-get update && apt-get install -y curl unzip && \
    rm -rf /var/lib/apt/lists/*

RUN wget https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz && tar xz -f scala-${SCALA_VERSION}.tgz -C /usr/local && ln -s /usr/local/scala-${SCALA_VERSION}/bin/scala /usr/local/bin/scala && ln -s /usr/local/scala-${SCALA_VERSION}/bin/scalac /usr/local/bin/scalac && ln -s /usr/local/scala-${SCALA_VERSION}/bin/scaladoc /usr/local/bin/scaladoc

RUN wget https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz && tar xz -f sbt-${SBT_VERSION}.tgz -C /usr/local &&  ln -s /usr/local/sbt/bin/sbt /usr/local/bin/sbt