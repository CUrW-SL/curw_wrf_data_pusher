#!/usr/bin/env bash

# Print execution date time
echo `date`

echo "Changing into ~/wrf_data_pusher"
cd /home/uwcc-admin/curw_wrf_data_pusher
echo "Inside `pwd`"


# If no venv (python3 virtual environment) exists, then create one.
if [ ! -d "venv" ]
then
    echo "Creating venv python3 virtual environment."
    virtualenv -p python3 venv
fi

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate

# Install dependencies using pip.
if [ ! -f "db.log" ]
then
    echo "Installing numpy"
    pip install numpy
    echo "Installing netCDF4"
    pip install netCDF4
    echo "Installing cftime"
    pip install cftime
    echo "Installing PyMySQL"
    pip install PyMySQL
    echo "Installing PyYAML"
    pip install PyYAML
    echo "Installing datalayer"
#    pip install git+https://github.com/shadhini/curw_db_adapter.git -U
    pip install git+https://github.com/shadhini/curw_db_adapter.git
fi

date=$1
config_file_path="config/mwrf_d0_18_config.json"
if [ -z $date ]
then
  date=$(date -u -d '+5 hour +30 min' '+%F')
fi

nohup ./wrf_data_pusher_seq.sh config/mwrf_config.json /mnt/disks/wrf_nfs/wrf d0 00 T5 2019-11-19

## Push WRF data into the database
echo "Running scripts to extract wrf data parallely. Logs Available in wrf_data_pusher.log file."
echo "Params passed :: config_file_path=$config_file_path, date=$date"
./wrf_data_pusher.py -c $config_file_path -D $date >> wrf_data_pusher_d0_18.log 2>&1


# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate
