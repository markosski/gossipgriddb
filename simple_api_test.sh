#!/bin/bash
set -x
INTERVAL=1
ID=NONE

time curl -H "Content-Type: application/json" -XPOST http://127.0.0.1:3001/items -d '{"partition_key": "123", "message": "foo1"}'
sleep $INTERVAL
time curl -H "Content-Type: application/json" -XPOST http://127.0.0.1:3002/items -d '{"partition_key": "124", "message": "foo2"}'
sleep $INTERVAL
time curl -H "Content-Type: application/json" -XPOST http://127.0.0.1:3003/items -d '{"partition_key": "125", "message": "foo3"}'
sleep $INTERVAL
time curl -H "Content-Type: application/json" -XPOST http://127.0.0.1:3002/items -d '{"partition_key": "124", "message": "foo4"}'
sleep $INTERVAL
time curl -H "Content-Type: application/json" -XDELETE http://127.0.0.1:3001/items/123