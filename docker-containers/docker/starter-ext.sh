#!/bin/bash
# Start - modified by HSG-ICS Interactions group  

if [ -n "$ENSURE_INPUT_DIR" ]; then
  export BCS_DS_OUTPUT_DIR=/output
  export BCS_DS_JARS_DIR=/jars
  sleep 8
  /opt/hadoop/bin/hadoop fs -mkdir -p $BCS_DS_JARS_DIR
  /opt/hadoop/bin/hadoop fs -mkdir -p $BCS_DS_OUTPUT_DIR/logs
  /opt/hadoop/bin/hadoop fs -mkdir -p $ENSURE_INPUT_DIR
  
  /opt/hadoop/bin/hadoop fs -chmod 777 -R $BCS_DS_OUTPUT_DIR

  
  if [ -n "$INPUT_FILE_DIR" ]; then
    /opt/hadoop/bin/hadoop fs -put -p $INPUT_FILE_DIR/* $ENSURE_INPUT_DIR
    # /opt/hadoop/bin/hadoop fs -put /root/app.jar /jars

  fi
fi
# Stop - modified by HSG-ICS Interactions group
