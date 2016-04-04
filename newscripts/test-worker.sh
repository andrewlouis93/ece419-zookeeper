#!/bin/bash

source test-base.sh

rm /tmp/louisan1/server1/data
if [ $# -ne 1 ]; then
    echo Please specify a profile under testcases.
    exit -1
fi

# testing setup
start_zk

ln -sf $1 inject-agent.properties

start_fileserver "fs"
start_jobtracker "jt"
start_worker_injected "worker-0"
for ((i=1;i<4;i+=1)); do
    start_worker "worker-$i"
done

run

kill_proc "fs"
kill_proc "jt"
for ((i=0;i<4;i++)); do
    kill_proc "worker-$i"
done

stop_zk
killall java

grep -i found *.result
