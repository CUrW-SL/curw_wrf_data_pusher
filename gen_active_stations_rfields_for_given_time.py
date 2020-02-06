#!/home/uwcc-admin/curw_wrf_data_pusher/venv/bin/python3
import traceback
import numpy as np
import os
import json
from datetime import datetime, timedelta
import time
import sys
import getopt
import pandas as pd


from db_adapter.base import get_Pool, destroy_Pool

from db_adapter.curw_fcst.source import get_source_id
from db_adapter.curw_fcst.variable import get_variable_id
from db_adapter.curw_fcst.unit import get_unit_id, UnitType
from db_adapter.curw_fcst.station import get_wrf_stations
from db_adapter.curw_fcst.timeseries import Timeseries as FCST_Timeseries
from db_adapter.curw_sim.grids import get_obs_to_d03_grid_mappings_for_rainfall, GridInterpolationEnum
from db_adapter.curw_sim.common import extract_obs_rain_15_min_ts
from db_adapter.constants import CURW_FCST_DATABASE, CURW_FCST_PASSWORD, CURW_FCST_USERNAME, CURW_FCST_PORT,\
    CURW_FCST_HOST
from db_adapter.constants import CURW_OBS_USERNAME, CURW_OBS_DATABASE, CURW_OBS_HOST, CURW_OBS_PASSWORD, CURW_OBS_PORT
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.logger import logger

SRI_LANKA_EXTENT = [79.5213, 5.91948, 81.879, 9.83506]
KELANI_BASIN_EXTENT = [79.6, 6.6, 81.0, 7.4]


email_content = {}

# local_output_root_dir = '/home/uwcc-admin/wrf_rfields'
# local_rfield_home = ''
bucket_rfield_home_d03 = ''
bucket_rfield_home_d03_kelani_basin = ''


def usage():
    usageText = """
    Usage: ./gen_active_stations_rfields_for_given_time.py -c [config_file_path] -d [wrf_root_directory] -r [gfs_run] -H [gfs_data_hour]
    -s [wrf_system] -D [date] -f [expected_fgt]

    -h  --help          Show usage
    -c  --config        Config file name or path. e.g: "wrf_config.json"
    -d  --dir           WRF root directory. e.g.: "/mnt/disks/wrf_nfs/wrf"
    -r  --run           GFS run. e.g: d0 (for yesterday gfs data), d1 (for today gfs data) 
    -H  --hour          GFS data hour. e.g: 00,06,12,18
    -s  --wrf_systems   List of WRF Systems. e.g.: A,C,E,SE
    -D  --date          Run date. e.g.: 2019-10-07 (date of the directory containing the wrf output to be used)
    -f  --fgt           Expected fgt of the forecasts

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
        exit(1)


def list_of_lists_to_df_first_row_as_columns(data):
    """

    :param data: data in list of lists format
    :return: equivalent pandas dataframe
    """

    return pd.DataFrame.from_records(data[1:], columns=data[0])


def write_to_file(file_name, data):
    with open(file_name, 'w+') as f:
        f.write('\n'.join(data))


def makedir_if_not_exist(dir_path):
    try:
        os.makedirs(dir_path)
    except FileExistsError:
        # directory already exists
        pass


def select_rectagular_sub_region(all_grids, lon_min=79.6, lon_max=81.0, lat_min=6.6, lat_max=7.4):
    # default is kelani basin

    selected_grids = all_grids.query('longitude >= {} & longitude <= {} & latitude >= {} & latitude <= {}'
                                     .format(lon_min, lon_max, lat_min, lat_max))

    return selected_grids


def extract_active_curw_obs_rainfall_stations(curw_obs_pool):
    """
        Extract currently active (active within last week) rainfall obs stations
        :return:
        """

    obs_stations = {}  # key: 'station_id', values: [['hash_id','station_name', 'latitude', 'longitude']]

    connection = curw_obs_pool.connection()

    try:

        with connection.cursor() as cursor1:
            cursor1.callproc(procname='getActiveRainfallObsStations')
            results = cursor1.fetchall()

            for result in results:
                obs_stations[str(result.get('station_id'))] = [result.get('hash_id'), result.get('station_name'),
                                     result.get('latitude'), result.get('longitude')]

        return obs_stations

    except Exception as ex:
        msg = "Exception occurred while retrieving active observational stations."
        logger.error(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        exit(1)
    finally:
        connection.close()


def prepare_active_obs_stations_based_rfield(curw_fcst_pool, curw_sim_pool, curw_obs_pool, tms_meta, config_data, active_obs_stations):

    try:
        grid_interpolation = GridInterpolationEnum.getAbbreviation(GridInterpolationEnum.MDPA)

        obs_to_d03_grid_mapping = get_obs_to_d03_grid_mappings_for_rainfall(pool=curw_sim_pool,
                                                                        grid_interpolation=grid_interpolation)
    except Exception:
        msg = "Exception occurred while loading rainfall obs station to d03 station grid maps."
        logger.error(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        exit(1)

    obs_to_d03_dict = {}

    for key in obs_to_d03_grid_mapping.keys():
        grid_id_parts = key.split("_")
        if len(grid_id_parts) == 4:
            obs_to_d03_dict[grid_id_parts[1]] = obs_to_d03_grid_mapping.get(key)[0]

    dataframe = pd.DataFrame()
    outer_df_initialized = False

    try:

        for obs_id in active_obs_stations.keys():
            print('obs id, ', obs_id)

            d03_station_id = obs_to_d03_dict.get(obs_id)
            latitude = active_obs_stations.get(obs_id)[2]
            longitude = active_obs_stations.get(obs_id)[3]
            hash_id = active_obs_stations.get(obs_id)[0]

            df = pd.DataFrame()
            df_initialized = False

            for wrf_system in config_data['wrf_system_list']:
                source_name = "{}_{}".format(config_data['model'], wrf_system)

                source_id = None

                try:
                    source_id = get_source_id(pool=curw_fcst_pool, model=source_name, version=tms_meta['version'])
                except Exception:
                    try:
                        time.sleep(3)
                        source_id = get_source_id(pool=curw_fcst_pool, model=source_name, version=tms_meta['version'])
                    except Exception:
                        msg = "Exception occurred while loading source meta data for WRF_{} from database.".format(wrf_system)
                        logger.error(msg)
                        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
                        exit(1)

                if source_id is not None:
                    FCST_TS = FCST_Timeseries(curw_fcst_pool)
                    fcst_ts = FCST_TS.get_nearest_timeseries(sim_tag=tms_meta['sim_tag'], station_id=d03_station_id,
                                                       source_id=source_id, variable_id=tms_meta['variable_id'],
                                                       unit_id=tms_meta['unit_id'], expected_fgt=tms_meta['fgt'])
                    fcst_ts.insert(0, ['time', source_name])
                    fcst_ts_df = list_of_lists_to_df_first_row_as_columns(fcst_ts)

                    if not df_initialized:
                        df = fcst_ts_df
                        df_initialized = True
                    else:
                        df = pd.merge(df, fcst_ts_df, how="outer", on='time')

            obs_start = (df['time'].min() - timedelta(minutes=10)).strftime(COMMON_DATE_TIME_FORMAT)

            obs_ts = extract_obs_rain_15_min_ts(connection=curw_obs_pool.connection(), id=hash_id, start_time=obs_start)
            obs_ts.insert(0, ['time', 'obs'])
            obs_ts_df = list_of_lists_to_df_first_row_as_columns(obs_ts)

            df = pd.merge(df, obs_ts_df, how="left", on='time')

            df['longitude'] = longitude
            df['latitude'] = latitude
            df.set_index(['time', 'longitude', 'latitude'], inplace=True)
            df = df.dropna()

            if not outer_df_initialized:
                dataframe = df
                outer_df_initialized = True
            else:
                dataframe = dataframe.append(df)

    except Exception:
        msg = "Exception occurred while processing hybrid rfield."
        logger.error(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        traceback.print_exc()
        exit(1)

    dataframe.sort_index(inplace=True)

    kelani_basin_df = select_rectagular_sub_region(all_grids=dataframe)

    try:
        # dataframe.to_csv(os.path.join(local_rfield_home,
        #                               '{}_{}_{}_{}_15min_hybrid_rfield.csv'.
        #                               format(config_data['wrf_type'], config_data['gfs_run'], config_data['gfs_data_hour'],
        #                                      '_'.join(config_data['wrf_system_list']))),
        #                  header=True, index=True)

        dataframe.to_csv(os.path.join(bucket_rfield_home_d03,
                                      '{}_{}_{}_{}_15min_hybrid_rfield.csv'.
                                      format(config_data['wrf_type'], config_data['gfs_run'], config_data['gfs_data_hour'],
                                             '_'.join(config_data['wrf_system_list']))),
                         header=True, index=True)

        kelani_basin_df.to_csv(os.path.join(bucket_rfield_home_d03_kelani_basin,
                                      '{}_{}_{}_{}_15min_hybrid_rfield.csv'.
                                      format(config_data['wrf_type'], config_data['gfs_run'],
                                             config_data['gfs_data_hour'],
                                             '_'.join(config_data['wrf_system_list']))),
                         header=True, index=True)
    except Exception:
        msg = "Exception occurred while saving rfields to file."
        logger.error(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        exit(1)


if __name__ == "__main__":

    """
    Config.json
    {
      "version": "4.0",
      "wrf_type": "dwrf",
    
      "model": "WRF",
    
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
    config_data = {}
    curw_fcst_pool = None
    curw_obs_pool = None
    curw_sim_pool = None

    try:

        config_path = None
        wrf_dir = None
        gfs_run = None
        gfs_data_hour = None
        wrf_systems = None
        date = None
        fgt = None

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:c:d:r:H:s:D:f:",
                                       ["help", "config=", "dir=", "run=", "hour=", "wrf_systems=", "date=", "fgt="])
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
            elif opt in ("-s", "--wrf_systems"):
                wrf_systems = arg.strip()
            elif opt in ("-D", "--date"):
                date = arg.strip()
            elif opt in ("-f", "--fgt"):
                fgt = arg.strip()

        if config_path is None:
            msg = "Config file name is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            exit(1)

        config = json.loads(open(config_path).read())

        # source details
        if wrf_dir is None:
            msg = "WRF root directory is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            exit(1)
        if gfs_run is None:
            msg = "GFS run is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            exit(1)
        if gfs_data_hour is None:
            msg = "GFS data hour is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            exit(1)
        if wrf_systems is None:
            msg = "WRF systems are not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            exit(1)
        if fgt is None:
            msg = "Expected fgt is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            exit(1)

        model = read_attribute_from_config_file('model', config)
        version = read_attribute_from_config_file('version', config)
        wrf_type = read_attribute_from_config_file('wrf_type', config)
        wrf_system_list = wrf_systems.split(',')

        # sim_tag
        sim_tag_prefix = ''
        if wrf_type != 'wrf':
            sim_tag_prefix = wrf_type + "_"
        sim_tag = sim_tag_prefix + 'gfs_{}_{}'.format(gfs_run, gfs_data_hour)

        # unit details
        unit = read_attribute_from_config_file('unit', config)
        unit_type = UnitType.getType(read_attribute_from_config_file('unit_type', config))

        # variable details
        variable = read_attribute_from_config_file('variable', config)

        if date is None:
            msg = "Run date is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            exit(1)

        curw_fcst_pool = get_Pool(host=CURW_FCST_HOST, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD,
                                  port=CURW_FCST_PORT, db=CURW_FCST_DATABASE)
        curw_obs_pool = get_Pool(host=CURW_OBS_HOST, port=CURW_OBS_PORT, user=CURW_OBS_USERNAME,
                                 password=CURW_OBS_PASSWORD, db=CURW_OBS_DATABASE)
        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                                 port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        try:
            wrf_v3_stations = get_wrf_stations(curw_fcst_pool)

            variable_id = get_variable_id(pool=curw_fcst_pool, variable=variable)
            unit_id = get_unit_id(pool=curw_fcst_pool, unit=unit, unit_type=unit_type)
        except Exception:
            msg = "Exception occurred while loading common metadata from database."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            exit(1)

        if date is None:
            msg = "Run date is not specified."
            logger.error(msg)
            email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
            exit(1)

        tms_meta = {
            'sim_tag': sim_tag,
            'version': version,
            'variable': variable,
            'unit': unit,
            'unit_type': unit_type.value,
            'variable_id': variable_id,
            'unit_id': unit_id,
            'fgt': fgt
        }
        config_data = {
            'model': model,
            'version': version,
            'date': date,
            'wrf_dir': wrf_dir,
            'gfs_run': gfs_run,
            'gfs_data_hour': gfs_data_hour,
            'wrf_system_list': wrf_system_list,
            'wrf_type': wrf_type
        }

        active_obs_stations = extract_active_curw_obs_rainfall_stations(curw_obs_pool)

        # local_rfield_home = os.path.join(local_output_root_dir, config_data['version'], config_data['gfs_run'],
        #                                  config_data['gfs_data_hour'], 'rfields', config_data['wrf_type'])

        bucket_rfield_home_d03 = os.path.join(config_data['wrf_dir'], config_data['version'], config_data['gfs_run'],
                                 config_data['gfs_data_hour'], config_data['date'], 'rfields',
                                 config_data['wrf_type'], 'd03')

        bucket_rfield_home_d03_kelani_basin = os.path.join(config_data['wrf_dir'], config_data['version'], config_data['gfs_run'],
                                              config_data['gfs_data_hour'], config_data['date'], 'rfields',
                                              config_data['wrf_type'], 'd03_kelani_basin')

        # make rfield directories
        # makedir_if_not_exist(local_rfield_home)
        makedir_if_not_exist(bucket_rfield_home_d03)
        makedir_if_not_exist(bucket_rfield_home_d03_kelani_basin)

        prepare_active_obs_stations_based_rfield(curw_fcst_pool=curw_fcst_pool, curw_sim_pool=curw_sim_pool,
                                                 curw_obs_pool=curw_obs_pool,
                                                 tms_meta=tms_meta, config_data=config_data,
                                                 active_obs_stations=active_obs_stations)

    except Exception as e:
        msg = 'Config data loading error.'
        logger.error(msg)
        email_content[datetime.now().strftime(COMMON_DATE_TIME_FORMAT)] = msg
        traceback.print_exc()
    finally:
        if curw_fcst_pool is not None:
            destroy_Pool(curw_fcst_pool)
        if curw_obs_pool is not None:
            destroy_Pool(curw_obs_pool)
        if curw_sim_pool is not None:
            destroy_Pool(curw_sim_pool)
        print("{} ::: Rfield Generation Process \n::: Email Content {} \n::: Config Data {}"
                    .format(datetime.now(), json.dumps(email_content), json.dumps(config_data)))

