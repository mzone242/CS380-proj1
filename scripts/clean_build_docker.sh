#!/bin/bash

sudo docker image rm $(sudo docker image ls --format '{{.Repository}} {{.ID}}' | grep 'sekwonlee' | awk '{print $2}')

cd dockerfiles

sudo docker build --network=host . -f base.dockerfile -t mzone242/raft:base
sudo docker push mzone242/raft:base

sudo docker build . -f client.dockerfile -t mzone242/raft:client
sudo docker push mzone242/raft:client

sudo docker build . -f frontend.dockerfile -t mzone242/raft:frontend
sudo docker push mzone242/raft:frontend

sudo docker build . -f server.dockerfile -t mzone242/raft:server
sudo docker push mzone242/raft:server

cd ..
