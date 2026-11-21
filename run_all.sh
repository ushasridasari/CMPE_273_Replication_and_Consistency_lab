#!/bin/bash
# start both replicas, run the demo, then stop everything.
# replica logs go to logs/ folder so we can look them later.

cd "$(dirname "$0")"
mkdir -p logs

python3 -u replica.py --role secondary --port 5002 > logs/secondary.log 2>&1 &
SEC_PID=$!
python3 -u replica.py --role primary --port 5001 --secondary http://localhost:5002 > logs/primary.log 2>&1 &
PRI_PID=$!

trap "kill $PRI_PID $SEC_PID 2>/dev/null" EXIT
sleep 1

python3 client.py demo | tee logs/client_demo.log

sleep 0.5
echo
echo "----- primary log -----"
cat logs/primary.log
echo
echo "----- secondary log -----"
cat logs/secondary.log
