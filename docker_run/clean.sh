#!/bin/bash

# clean.sh <n_nodes>

N_NODES=$1

for ((i=0;i<$N_NODES;i++))
do
    docker rm node$i &
done
wait
docker network rm dpo