#!/usr/bin/env bash

NOWCAST_WORKFLOW_DIR=`pwd`
DAXFILES="$NOWCAST_WORKFLOW_DIR/testcase_daxs/*"

for DAXFILE in $DAXFILES; do
   echo "$(date) ---- Planning Daxfile: $DAXFILE"
   $NOWCAST_WORKFLOW_DIR/plan.sh $DAXFILE
   
   sleep 60
done
