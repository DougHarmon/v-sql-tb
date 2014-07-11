#!/bin/bash
export APPHOME=/home/dbadmin

. $APPHOME/.bashrc

export TODAY=$(date +%Y%m%d%H%M%S)
export SQLDIR=$APPHOME/vstb/sql/merge
export LOGDIR=$APPHOME/vstb/log
export VSQLDIR=/opt/vertica/bin

$VSQLDIR/vsql -f $SQLDIR/load_vstb_tables.sql > $LOGDIR/load_vstb_tables.${TODAY}.log
