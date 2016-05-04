#!/bin/bash

set -e

trap "trap - SIGTERM && kill -- -$$ 2>/dev/null" SIGINT SIGTERM EXIT

rm -f spa-server.log

./spa-server 0 2>&1 | tee -a spa-server.log &
./spa-server 1 2>&1 | tee -a spa-server.log &
./spa-server 2 2>&1 | tee -a spa-server.log &

wait
