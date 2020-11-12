#!/bin/bash
cd /apps/ANN/production
log=/apps/ANN/production/logs/hourly.log

dd=`date +%F -d'55 minutes ago'`
hh=`date +%H -d'55 minutes ago'`

#dd=`date +%F -d'5 hours ago'`
#hh=`date +%H -d'5 hours ago'`

echo "[ `date +%F_%T` ] Process start. date=$dd, hour=$hh" >> $log
./hourly_prediction.py $dd $hh >> logs/$hh.log
echo "[ `date +%F_%T` ] Process end" >> $log
