#!/bin/sh

# Pass docker CMD to actual entrypoint
/opt/bitnami/scripts/spark/entrypoint.sh $@ & 

# Needs better logic
sleep 16
/opt/bitnami/spark/sbin/start-history-server.sh

# Wait for all parallel jobs to finish
wait
