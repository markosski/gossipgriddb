#!/bin/bash
set -e
echo "Starting cluster"

RUST_LOG="info,gossipgrid::gossip=info"

# start the node and not wait for it to finish
GOSSIPGRID_BASE_DIR="gossipgrid_data/1" RUST_LOG="${RUST_LOG}" cargo run start --ephemeral -s3 -r2 -p64 -w3001 -h127.0.0.1:4109 --functions functions.json &
PID1=$!
GOSSIPGRID_BASE_DIR="gossipgrid_data/2" RUST_LOG="${RUST_LOG}" cargo run join --ephemeral -w3002 -h127.0.0.1:4110 -a127.0.0.1:4109 --functions functions.json &
PID2=$!
GOSSIPGRID_BASE_DIR="gossipgrid_data/3" RUST_LOG="${RUST_LOG}" cargo run join --ephemeral -w3003 -h127.0.0.1:4111 -a127.0.0.1:4109 --functions functions.json &
PID3=$!

trap "kill $PID1 $PID2 $PID3" EXIT

wait
