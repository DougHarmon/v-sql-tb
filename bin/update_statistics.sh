#!/bin/bash

export APPHOME=/home/dbadmin

. $APPHOME/.bashrc

export TODAY=$(date +%Y%m%d%H%M%S)
export SQLDIR=$APPHOME/vstb/sql/merge
export LOGDIR=$APPHOME/vstb/log
export VSQLDIR=/opt/vertica/bin

$VSQLDIR/vsql -f $SQLDIR/update_statistics.sql > $LOGDIR/update_statistics.${TODAY}.log
