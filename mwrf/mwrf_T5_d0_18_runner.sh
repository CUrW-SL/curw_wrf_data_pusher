#!/usr/bin/env bash

echo "Running mwrf T5 ... "

date=$1
if [ -z $date ]
then
  date=$(date -u -d '-1 day' '+%F')
fi

gfs_hour="18"
session_id=""

IFS='-' read -ra DATELIST <<< "$date"
for i in "${DATELIST[@]}"; do
    session_id="$session_id$i"
done
session_id="$session_id$gfs_hour"

echo $session_id

echo "/home/muditha/WRF/Build_WRF/run_researchwrfcurw.sh $session_id trial5 trial5 072 20 >&1 |tee /home/muditha/WRF/Build_WRF/job.log"

/home/muditha/WRF/Build_WRF/run_researchwrfcurw.sh $session_id trial5 trial5 072 20 >&1 |tee /home/muditha/WRF/Build_WRF/job.log

echo "Extracting d03_RAINNC.nc ... "

echo "Changing into ~/python"
cd /home/muditha/python
echo "Inside `pwd`"


# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate


wrf_version="4.0"
gfs_run="d0"
model="T5"

domain="trial5"
run_parameter="trial5"
run_hours="072"
num_of_processors_utilized="20"

run_start=$date

### Push WRF data to bucket
echo "Running scripts to extract wrf data parallely. Logs Available in extract.log file."
python extract_n_export_mwrf.py $wrf_version $gfs_run $gfs_hour $model $domain $run_parameter $run_hours $num_of_processors_utilized $run_start >> extract.log 2>&1


# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate