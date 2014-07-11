#!/bin/bash

export TODAY=$(date +%Y%m%d%H%M%S)
export SQLDIR=/home/dbadmin/vstb/sql/merge
export LOGDIR=/home/dbadmin/vstb/log
export VSQLDIR=/opt/vertica/bin

$VSQLDIR/vsql -f $SQLDIR/load_vstb_tables.sql > $LOGDIR/load_vstb_tables.${TODAY}.log
