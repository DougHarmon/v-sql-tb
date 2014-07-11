#!/bin/bash
#Monitor the lag in the Ancient History Mark (AHM)
#If AHM >= $HOURS_BEHIND then send an email to $EMAIL_LIST
export APPHOME=/home/dbabmin

. $APPHOME/.bashrc


export TODAY=$(date +%Y%m%d%H%M%S)
 

#Parameters
export HOURS_BEHIND=4
export EMAIL_SENDER='admin_email@nospam.com' 
export EMAIL_LIST='user1@spamme.com,user2@spamme.com' 
export VSQL_HOST=localhost
export VSQL_USER=dbadmin
export VSQL_DATABASE=VMart

export AHM_TIME=$(vsql -t -c 'SELECT GET_AHM_TIME()' | cut -c19-38)
echo $AHM_TIME

export CURRENT_TIME=$(vsql -t -c 'SELECT GETDATE()' | cut -c1-20)
echo $CURRENT_TIME

export LAG_HOURS=$(vsql -t -c "SELECT DATEDIFF(hour, '$AHM_TIME'::TIMESTAMP, GETDATE())")
echo $LAG_HOURS



if [ $LAG_HOURS -ge $HOURS_BEHIND ]; then
/usr/sbin/sendmail "$EMAIL_LIST" <<EOF
subject:"AHM is $LAG_HOURS hours behind on $VSQL_DATABASE !"
from:$EMAIL_SENDER
HOST          = $VSQL_HOST
DATABASE      = $VSQL_DATABASE
AHM           = $AHM_TIME
CURRENT_TIME  = $CURRENT_TIME
LAG_HOURS     = $LAG_HOURS
EOF
fi

exit
