#!/bin/bash

DPO_FLAGS=$1

timestamp() {
  date +"%T" # current time
}

for n_nodes in 4 7
do
    echo "$(timestamp)|Starting experiment with $n_nodes"
    mkdir -p $PWD/benchmark/$n_nodes
    ./start.sh $n_nodes $PWD/benchmark/$n_nodes "$DPO_FLAGS" --rm
    sleep 60
    ./stop.sh $n_nodes
done