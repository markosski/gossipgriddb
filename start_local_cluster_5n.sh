#!/bin/bash
set -e
echo "Starting cluster"

RUST_LOG="warn,gossipgrid::gossip=info"

# start the node and not wait for it to finish
GOSSIPGRID_BASE_DIR="gossipgrid_data/1" RUST_LOG="${RUST_LOG}" cargo run start --ephemeral -s5 -r2 -p15 -w3001 -h127.0.0.1:4109&
PID1=$!
GOSSIPGRID_BASE_DIR="gossipgrid_data/2" RUST_LOG="${RUST_LOG}" cargo run join --ephemeral -w3002 -h127.0.0.1:4110 -a127.0.0.1:4109 &
PID2=$!
GOSSIPGRID_BASE_DIR="gossipgrid_data/3" RUST_LOG="${RUST_LOG}" cargo run join --ephemeral -w3003 -h127.0.0.1:4111 -a127.0.0.1:4109 &
PID3=$!
GOSSIPGRID_BASE_DIR="gossipgrid_data/4" RUST_LOG="${RUST_LOG}" cargo run join --ephemeral -w3004 -h127.0.0.1:4112 -a127.0.0.1:4109 &
PID4=$!
GOSSIPGRID_BASE_DIR="gossipgrid_data/5" RUST_LOG="${RUST_LOG}" cargo run join --ephemeral -w3005 -h127.0.0.1:4113 -a127.0.0.1:4109 &
PID5=$!

trap "kill $PID1 $PID2 $PID3 $PID4 $PID5" EXIT

wait
