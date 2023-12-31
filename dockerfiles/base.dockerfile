FROM ubuntu:20.04

MAINTAINER Sekwon Lee <sklee@cs.utexas.edu> version: 0.1

USER root

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

RUN apt-get update && apt-get install -y git

RUN git clone https://github.com/mzone242/CS380-proj1.git

ENV KVS_HOME /CS380-proj1

# Install dependencies
WORKDIR ${KVS_HOME}/scripts
RUN bash dependencies2.sh

WORKDIR /
