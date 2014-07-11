#!/bin/bash

export TODAY=$(date +%Y%m%d%H%M%S)
export SQLDIR=/home/dbadmin/vstb/sql/merge
export LOGDIR=/home/dbadmin/vstb/log
export VSQLDIR=/opt/vertica/bin

$VSQLDIR/vsql -f $SQLDIR/update_statistics.sql > $LOGDIR/update_statistics.${TODAY}.log
