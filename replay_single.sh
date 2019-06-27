#!/usr/bin/env bash

NOWCAST_WORKFLOW_DIR=`pwd`
DAXFILES="$NOWCAST_WORKFLOW_DIR/testcase_daxs/*"

for DAXFILE in $DAXFILES; do
   echo "$(date) ---- Planning Daxfile: $DAXFILE"
   $NOWCAST_WORKFLOW_DIR/plan.sh $DAXFILE
   
   while [[ "$(pegasus-status | head -n 1)" != "(no matching jobs found in Condor Q)" ]]; do
      sleep 300
   done
done
