#!/bin/bash

# start.sh <n_nodes> <tx_per_second> <docker_flags>

# use --rm flag for docker in order to remove them automatically with stop.sh

N_NODES=$1
BENCHMARK_FOLDER=$2
DPO_FLAGS=$3
DOCKER_FLAGS=$4

PEERS_FILE=$PWD/peers_docker.json

rm $PEERS_FILE > /dev/null
echo -n "[\"tcp://node0:4000\"" >> $PEERS_FILE
for ((i=1;i<$N_NODES;i++))
do
    echo -n ",\"tcp://node$i:4000\"" >> $PEERS_FILE
done
echo -n "]" >> $PEERS_FILE
echo "Created $PEERS_FILE file"

docker network create dpo
for ((i=0;i<$N_NODES;i++))
do
    docker run \
        -d --network dpo --name node$i \
        -v $PEERS_FILE:/dpo/peers.json:ro \
        -v $BENCHMARK_FOLDER:/dpo/benchmark \
        $DOCKER_FLAGS \
        dpo -id $i -peers /dpo/peers.json -benchmark /dpo/benchmark/node$i.csv $DPO_FLAGS &
done
wait