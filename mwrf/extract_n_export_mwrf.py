from datetime import datetime, timedelta
import xarray as xr
import os
import sys


args = sys.argv

wrf_version = args[1] #4.0
gfs_run = args[2] #d0
gfs_hour = args[3] #18
model = args[4] #T5

domain= args[5] #trial5
run_parameter= args[6] #trial5
run_hours= args[7] #072
num_of_processors_utilized = args[8] #20

run_start = args[9] #2019-12-01

run_session = (str(run_start)).replace('-','') + gfs_hour #2019121518


def makedir_if_not_exist(dir_path):
    try:
        os.makedirs(dir_path)
    except FileExistsError:
        # directory already exists
        pass


output_dir_path = "/home/muditha/WRF/Build_WRF/EXPORT/{}_{}_{}_{}".format(domain, run_parameter, run_session, run_hours)

# nc_f = "/home/muditha/WRF/Build_WRF/EXPORT/trial5_trial5_2019120118_072/wrfout_d03_2019-12-01_18-00-00"
# /home/muditha/WRF/Build_WRF/EXPORT/trial5_trial5_2019122118_072/wrfout_d03_2019-12-21_18-00-00
d03_nc_f = "{}/wrfout_d03_{}_{}-00-00"\
    .format(output_dir_path, run_start, gfs_hour)
d01_nc_f = "{}/wrfout_d01_{}_{}-00-00"\
    .format(output_dir_path, run_start, gfs_hour)

#Save d03 and d01 RAINNC
ds3 = xr.open_dataset(d03_nc_f, engine="netcdf4")
ds1 = xr.open_dataset(d01_nc_f, engine="netcdf4")
ds3.RAINNC.to_netcdf(path="{}/d03_RAINNC.nc".format(output_dir_path), engine="scipy")
ds1.RAINNC.to_netcdf(path="{}/d01_RAINNC.nc".format(output_dir_path), engine="scipy")
# ds.U10.to_netcdf(path="d03_U10.nc",engine="scipy")
# ds.V10.to_netcdf(path="d03_V10.nc",engine="scipy")
# ds.U[:,0,:,:].to_netcdf(path="d03_U1.nc",engine="scipy")
# ds.V[:,0,:,:].to_netcdf(path="d03_V1.nc",engine="scipy")

output_date = datetime.now().strftime('%Y-%m-%d')

local_d03_RAINNC_file_path = "{}/d03_RAINNC.nc".format(output_dir_path)
local_d01_RAINNC_file_path = "{}/d01_RAINNC.nc".format(output_dir_path)

local_bucket_mnt_dir = "/home/muditha/gbucket"
bucket_output_dir = "{}/wrf/{}/{}/{}/{}/output/mwrf/{}/".format(local_bucket_mnt_dir, wrf_version, gfs_run, gfs_hour, output_date, model)

makedir_if_not_exist(bucket_output_dir)

os.system("cp {} {}".format(local_d03_RAINNC_file_path, bucket_output_dir))
os.system("cp {} {}".format(local_d01_RAINNC_file_path, bucket_output_dir))

# remove unwanted files
os.system("rm {}/FILE*".format(output_dir_path))
os.system("rm {}/geo_em*".format(output_dir_path))
os.system("rm {}/met_em*".format(output_dir_path))
os.system("rm {}/namelist.*".format(output_dir_path))
os.system("rm {}/rsl.*".format(output_dir_path))
os.system("rm {}/wrfbdy*".format(output_dir_path))
os.system("rm {}/wrfinput*".format(output_dir_path))



