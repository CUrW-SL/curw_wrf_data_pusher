#!/home/uwcc-admin/curw_wrf_data_pusher/venv/bin/python3
# generate rfields for a given run date
import traceback
from netCDF4 import Dataset
import numpy as np
import os
import json
from datetime import datetime, timedelta
import time
import sys
import getopt
import pandas as pd


from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.logger import logger

SRI_LANKA_EXTENT = [79.5213, 5.91948, 81.879, 9.83506]
KELANI_BASIN_EXTENT = [79.6, 6.6, 81.0, 7.4]


email_content = {}
local_output_root_dir = '/home/uwcc-admin/wrf_rfields'
bucket_rfield_home = ''
d03_kelani_basin_rfield_home = ''
d03_rfield_home = ''
d01_rfield_home = ''


def usage():
    usageText = """
    Usage: ./gen_rfields.py -c [config_file_path] -d [wrf_root_directory] -r [gfs_run] -H [gfs_data_hour]
    -s [wrf_system] -D [date] 

    -h  --help          Show usage
    -c  --config        Config file name or path. e.g: "wrf_config.json"
    -d  --dir           WRF root directory. e.g.: "/mnt/disks/wrf_nfs/wrf"
    -r  --run           GFS run. e.g: d0 (for yesterday gfs data), d1 (for today gfs data) 
    -H  --hour          GFS data hour. e.g: 00,06,12,18
    -s  --wrf_system    WRF System. e.g.: A,C,E,SE
    -D  --date          Run date. e.g.: 2019-10-07 (date of the directory containing the wrf output to be used)

    """
    print(usageText)


def read_attribute_from_config_file(attribute, config):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :return:
    """

    if attribute in config and (config[attribute] != ""):
        return config[attribute]
    else:
        msg = "{} not specified in config file.".format(attribute)
        logger.error(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        sys.exit(1)


def list_of_lists_to_df_first_row_as_columns(data):
    """

    :param data: data in list of lists format
    :return: equivalent pandas dataframe
    """
    return pd.DataFrame.from_records(data[1:], columns=data[0])


def get_per_time_slot_values(prcp):
    per_interval_prcp = (prcp[1:] - prcp[:-1])
    return per_interval_prcp


def get_file_last_modified_time(file_path):
    # returns local time (UTC + 5 30)
    modified_time = time.gmtime(os.path.getmtime(file_path) + 19800)

    return time.strftime('%Y-%m-%d %H:%M:%S', modified_time)


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + timedelta(hours=5, minutes=30 + shift_mins)


def write_to_file(file_name, data):
    with open(file_name, 'w+') as f:
        f.write('\n'.join(data))


def makedir_if_not_exist(dir_path):
    try:
        os.makedirs(dir_path)
    except FileExistsError:
        # directory already exists
        pass


def remove_all_files(dir):
    os.system("rm -f {}/*".format(dir))


def zip_folder(source, destination):
    os.system("tar -C {} -czf {}.tar.gz {}".format('/'.join(source.split('/')[:-1]), destination, source.split('/')[-1]))


def create_d03_rfields(d03_rainnc_netcdf_file_path, config_data):
    """

    :param d03_rainnc_net_cdf_file_path:
    :return:

    rainc_unit_info:  mm
    lat_unit_info:  degree_north
    time_unit_info:  minutes since 2019-04-02T18:00:00
    """
    if not os.path.exists(d03_rainnc_netcdf_file_path):
        msg = 'no d03 rainnc netcdf :: {}'.format(d03_rainnc_netcdf_file_path)
        logger.warning(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        return False
    else:

        try:
            """
            RAINNC netcdf data extraction

            """

            nnc_fid = Dataset(d03_rainnc_netcdf_file_path, mode='r')

            time_unit_info = nnc_fid.variables['XTIME'].description

            time_unit_info_list = time_unit_info.split('since ')

            lats = nnc_fid.variables['XLAT'][0, :, 0]
            lons = nnc_fid.variables['XLONG'][0, 0, :]

            lon_min = lons[0].item()
            lat_min = lats[0].item()
            lon_max = lons[-1].item()
            lat_max = lats[-1].item()

            lat_inds = np.where((lats >= lat_min) & (lats <= lat_max))
            lon_inds = np.where((lons >= lon_min) & (lons <= lon_max))

            rainnc = nnc_fid.variables['RAINNC'][:, lat_inds[0], lon_inds[0]]

            times = nnc_fid.variables['XTIME'][:]

            nnc_fid.close()

            diff = get_per_time_slot_values(rainnc)

            width = len(lons)
            height = len(lats)

            xy = False

            for i in range(len(diff)):
                ts_time = datetime.strptime(time_unit_info_list[1], '%Y-%m-%d %H:%M:%S') + timedelta(
                    minutes=times[i + 1].item())
                timestamp = datetime_utc_to_lk(ts_time, shift_mins=0)

                rfield = [['longitude', 'latitude', 'value']]
                for y in range(height):
                    for x in range(width):

                        lat = float('%.6f' % lats[y])
                        lon = float('%.6f' % lons[x])

                        rfield.append([lon, lat, float('%.3f' % diff[i, y, x])])

                rfield_df = list_of_lists_to_df_first_row_as_columns(rfield).sort_values(['longitude', 'latitude'], ascending=[True, True])

                KB_lon_min = KELANI_BASIN_EXTENT[0]
                KB_lat_min = KELANI_BASIN_EXTENT[1]
                KB_lon_max = KELANI_BASIN_EXTENT[2]
                KB_lat_max = KELANI_BASIN_EXTENT[3]

                kelani_basin_df = rfield_df[(rfield_df.longitude >= KB_lon_min) & (rfield_df.longitude <= KB_lon_max) &
                                         (rfield_df.latitude >= KB_lat_min) & (rfield_df.latitude <= KB_lat_max)]

                print(rfield_df)

                if not xy:
                    rfield_df.to_csv(os.path.join(d03_rfield_home, 'xy.csv'), columns=['longitude', 'latitude'], header=False, index=None)

                    kelani_basin_df.to_csv(os.path.join(d03_kelani_basin_rfield_home, 'xy.csv'), columns=['longitude', 'latitude'], header=False, index=None)

                    xy = True

                rfield_df.to_csv(os.path.join(d03_rfield_home, "{}_{}_{}_{}.txt".format(config_data['model'], config_data['wrf_system'], config_data['version'], timestamp.strftime('%Y-%m-%d_%H-%M'))),
                                 columns=['value'], header=False, index=None)

                kelani_basin_df.to_csv(os.path.join(d03_kelani_basin_rfield_home, "{}_{}_{}_{}.txt".format(config_data['model'], config_data['wrf_system'], config_data['version'], timestamp.strftime('%Y-%m-%d_%H-%M'))),
                                       columns=['value'], header=False, index=None)

            zip_folder(d03_kelani_basin_rfield_home, os.path.join(bucket_rfield_home, "kelani_basin"))
            zip_folder(d03_rfield_home, os.path.join(bucket_rfield_home, 'd03'))

            return True
        except Exception as e:
            msg = "netcdf file at {} reading error.".format(d03_rainnc_netcdf_file_path)
            logger.error(msg)
            traceback.print_exc()
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            return False


def create_d01_rfields(d01_rainnc_netcdf_file_path, config_data):
    """

    :param d03_rainnc_net_cdf_file_path:
    :return:

    rainc_unit_info:  mm
    lat_unit_info:  degree_north
    time_unit_info:  minutes since 2019-04-02T18:00:00
    """
    if not os.path.exists(d01_rainnc_netcdf_file_path):
        msg = 'no d01 rainnc netcdf :: {}'.format(d01_rainnc_netcdf_file_path)
        logger.warning(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        return False
    else:

        try:
            """
            RAINNC netcdf data extraction

            """

            nnc_fid = Dataset(d01_rainnc_netcdf_file_path, mode='r')

            time_unit_info = nnc_fid.variables['XTIME'].description

            time_unit_info_list = time_unit_info.split('since ')

            lats = nnc_fid.variables['XLAT'][0, :, 0]
            lons = nnc_fid.variables['XLONG'][0, 0, :]

            lon_min = lons[0].item()
            lat_min = lats[0].item()
            lon_max = lons[-1].item()
            lat_max = lats[-1].item()

            lat_inds = np.where((lats >= lat_min) & (lats <= lat_max))
            lon_inds = np.where((lons >= lon_min) & (lons <= lon_max))

            rainnc = nnc_fid.variables['RAINNC'][:, lat_inds[0], lon_inds[0]]

            times = nnc_fid.variables['XTIME'][:]

            nnc_fid.close()

            diff = get_per_time_slot_values(rainnc)

            width = len(lons)
            height = len(lats)

            xy = False

            for i in range(len(diff)):
                ts_time = datetime.strptime(time_unit_info_list[1], '%Y-%m-%d %H:%M:%S') + timedelta(
                    minutes=times[i + 1].item())
                timestamp = datetime_utc_to_lk(ts_time, shift_mins=0)

                rfield = [['longitude', 'latitude', 'value']]
                for y in range(height):
                    for x in range(width):

                        lat = float('%.6f' % lats[y])
                        lon = float('%.6f' % lons[x])

                        rfield.append([lon, lat, float('%.3f' % diff[i, y, x])])

                rfield_df = list_of_lists_to_df_first_row_as_columns(rfield).sort_values(['longitude', 'latitude'], ascending=[True, True])

                if not xy:
                    rfield_df.to_csv(os.path.join(d03_rfield_home, 'xy.csv'), columns=['longitude', 'latitude'], header=False, index=None)
                    xy = True

                rfield_df.to_csv(os.path.join(d03_rfield_home, "{}_{}_{}_{}.txt".format(config_data['model'], config_data['wrf_system'], config_data['version'], timestamp.strftime('%Y-%m-%d_%H-%M'))),
                                 columns=['value'], header=False, index=None)

            zip_folder(d01_rfield_home, os.path.join(bucket_rfield_home, 'd01'))
            return True
        except Exception as e:
            msg = "netcdf file at {} reading error.".format(d01_rainnc_netcdf_file_path)
            logger.error(msg)
            traceback.print_exc()
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            return False


if __name__ == "__main__":
    # config_data = {
    #             'model': "WRF",
    #             'version': "4.0",
    #             'wrf_system': "A"
    #         }
    # create_d03_rfields("/home/shadhini/dev/repos/curw-sl/curw_wrf_data_pusher/wrf_4.0_18_A_2019-10-15_d03_RAINNC.nc", config_data)

    """
    Config.json
    {
      "version": "4.0",

      "model": "WRF",

      "sim_tag": "gfs_d0_18",

      "unit": "mm",
      "unit_type": "Accumulative",

      "variable": "Precipitation"

    }

    /wrf_nfs/wrf/4.0/d0/18/A/2019-07-30/d03_RAINNC.nc

    tms_meta = {
                    'sim_tag'       : sim_tag,
                    'latitude'      : latitude,
                    'longitude'     : longitude,
                    'model'         : model,
                    'version'       : version,
                    'variable'      : variable,
                    'unit'          : unit,
                    'unit_type'     : unit_type
                    }
    """
    try:

        config_path = None
        wrf_dir = None
        gfs_run = None
        gfs_data_hour = None
        wrf_system = None
        date = None

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:c:d:r:H:s:D:",
                                       ["help", "config=", "dir=", "run=", "hour=", "wrf_system=", "date="])
        except getopt.GetoptError:
            usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage()
                sys.exit()
            elif opt in ("-c", "--config"):
                config_path = arg.strip()
            elif opt in ("-d", "--dir"):
                wrf_dir = arg.strip()
            elif opt in ("-r", "--run"):
                gfs_run = arg.strip()
            elif opt in ("-H", "--hour"):
                gfs_data_hour = arg.strip()
            elif opt in ("-s", "--wrf_system"):
                wrf_system = arg.strip()
            elif opt in ("-D", "--date"):
                date = arg.strip()

        if config_path is None:
            msg = "Config file name is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(1)

        config = json.loads(open(config_path).read())

        # source details
        if wrf_dir is None:
            msg = "WRF root directory is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(1)
        if gfs_run is None:
            msg = "GFS run is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(1)
        if gfs_data_hour is None:
            msg = "GFS data hour is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(1)
        if wrf_system is None:
            msg = "WRF system is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(1)
        model = read_attribute_from_config_file('model', config)
        version = read_attribute_from_config_file('version', config)
        is_docker = read_attribute_from_config_file('is_docker', config)

        if date is None:
            msg = "Run date is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            sys.exit(1)

        config_data = {
            'model': model,
            'version': version,
            'date': date,
            'wrf_dir': wrf_dir,
            'gfs_run': gfs_run,
            'gfs_data_hour': gfs_data_hour,
            'wrf_system': wrf_system
        }

        output_dir = os.path.join(config_data['wrf_dir'], config_data['version'], config_data['gfs_run'],
                                  config_data['gfs_data_hour'], config_data['date'], wrf_system)

        if is_docker:
            d03_kelani_basin_rfield_home = os.path.join(local_output_root_dir, 'dwrf',  config_data['version'],
                                                        config_data['gfs_run'], config_data['gfs_data_hour'],
                                                        wrf_system, 'rfield/d03_kelani_basin')
            d03_rfield_home = os.path.join(local_output_root_dir, 'dwrf', config_data['version'],
                                                        config_data['gfs_run'], config_data['gfs_data_hour'],
                                                        wrf_system, 'rfield/d03')
            d01_rfield_home = os.path.join(local_output_root_dir, 'dwrf', config_data['version'],
                                           config_data['gfs_run'], config_data['gfs_data_hour'],
                                           wrf_system, 'rfield/d01')
        else:
            d03_kelani_basin_rfield_home = os.path.join(local_output_root_dir, 'wrf', config_data['version'],
                                                        config_data['gfs_run'], config_data['gfs_data_hour'],
                                                        wrf_system, 'rfield/d03_kelani_basin')
            d03_rfield_home = os.path.join(local_output_root_dir, 'wrf', config_data['version'],
                                           config_data['gfs_run'], config_data['gfs_data_hour'],
                                           wrf_system, 'rfield/d03')
            d01_rfield_home = os.path.join(local_output_root_dir, 'wrf', config_data['version'],
                                           config_data['gfs_run'], config_data['gfs_data_hour'],
                                           wrf_system, 'rfield/d01')

        bucket_rfield_home = os.path.join(output_dir, 'rfield')

        # remove older files
        remove_all_files(d03_kelani_basin_rfield_home)
        remove_all_files(d03_rfield_home)
        remove_all_files(d01_rfield_home)

        # make local rfield directories
        makedir_if_not_exist(d03_kelani_basin_rfield_home)
        makedir_if_not_exist(d03_rfield_home)
        makedir_if_not_exist(d01_rfield_home)

        # make bucket rfield directory
        makedir_if_not_exist(bucket_rfield_home)

        d03_rainnc_netcdf_file = 'd03_RAINNC.nc'
        d03_rainnc_netcdf_file_path = os.path.join(output_dir, d03_rainnc_netcdf_file)
        d01_rainnc_netcdf_file = 'd01_RAINNC.nc'
        d01_rainnc_netcdf_file_path = os.path.join(output_dir, d01_rainnc_netcdf_file)

        create_d03_rfields(d03_rainnc_netcdf_file_path, config_data)
        create_d01_rfields(d01_rainnc_netcdf_file_path, config_data)


    except Exception as e:
        msg = 'Multiprocessing error.'
        logger.error(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        traceback.print_exc()
    finally:
        logger.info("Process finished.")
        logger.info("Email Content {}".format(json.dumps(email_content)))

