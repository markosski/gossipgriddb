#!/bin/bash
set -e
echo "Starting cluster"

RUST_LOG="info,gossipgrid::gossip=info"

# register new cluster if doesn't exist (idempotent)
GOSSIPGRID_BASE_DIR="gossipgrid_data/1" RUST_LOG="${RUST_LOG}" cargo run create --name=3node --size=3 --partitions=9 --replication=2

# start the node and not wait for it to finish
GOSSIPGRID_BASE_DIR="gossipgrid_data/1" RUST_LOG="${RUST_LOG}" cargo run start --name=3node -w3001 -h127.0.0.1:4109&
PID1=$!
GOSSIPGRID_BASE_DIR="gossipgrid_data/2" RUST_LOG="${RUST_LOG}" cargo run join --name=3node -w3002 -h127.0.0.1:4110 -a127.0.0.1:4109 &
PID2=$!
GOSSIPGRID_BASE_DIR="gossipgrid_data/3" RUST_LOG="${RUST_LOG}" cargo run join --name=3node -w3003 -h127.0.0.1:4111 -a127.0.0.1:4109 &
PID3=$!

trap "kill $PID1 $PID2 $PID3" EXIT

wait
