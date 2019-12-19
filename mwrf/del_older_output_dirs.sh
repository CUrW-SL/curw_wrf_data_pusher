#!/bin/bash

NOW=$(date +%s)

cd /home/muditha/WRF/Build_WRF/EXPORT

for dir in $(ls -d */)
do
  #Do something, the directory is accessible with $d:
  LAST_MODIFIED_DATE=$(date -r $dir +%s)
  DIFF=$((((((NOW-LAST_MODIFIED_DATE)/60)/60)/24)))
  prefix="${dir:0:13}"
#  echo $prefix
  if [ $prefix = "trial5_trial5" ]
  then
    echo $dir
    if [ $DIFF -gt 5 ]
    then
      echo $DIFF
      rm -vr $dir
    fi
  fi

done