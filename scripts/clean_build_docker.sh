#!/bin/bash

sudo docker image rm $(sudo docker image ls --format '{{.Repository}} {{.ID}}' | grep 'mzone242' | awk '{print $2}')

sudo docker build --network=host . -f dockerfiles/base.dockerfile -t mzone242/raft:base
sudo docker push mzone242/raft:base

sudo docker build . -f dockerfiles/client.dockerfile -t mzone242/raft:client
sudo docker push mzone242/raft:client

sudo docker build . -f dockerfiles/frontend.dockerfile -t mzone242/raft:frontend
sudo docker push mzone242/raft:frontend

sudo docker build . -f dockerfiles/server.dockerfile -t mzone242/raft:server
sudo docker push mzone242/raft:server
