#!/bin/sh

/opt/starter.sh hdfs datanode & /opt/starter-ext.sh

# Wait for all parallel jobs to finish
wait
