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


nc_f = "/home/muditha/WRF/Build_WRF/EXPORT/{}_{}_{}_{}/wrfout_d03_{}_{}-00-00"\
    .format(domain, run_parameter, run_session, run_hours, run_start, gfs_hour)
#Save RAINNC and RAINC
ds = xr.open_dataset(nc_f, engine="netcdf4")
ds.RAINNC.to_netcdf(path="d03_RAINNC.nc",engine="scipy")
# ds.U10.to_netcdf(path="d03_U10.nc",engine="scipy")
# ds.V10.to_netcdf(path="d03_V10.nc",engine="scipy")
# ds.U[:,0,:,:].to_netcdf(path="d03_U1.nc",engine="scipy")
# ds.V[:,0,:,:].to_netcdf(path="d03_V1.nc",engine="scipy")

output_date = datetime.now().strftime('%Y-%m-%d')

local_d03_RAINNC_file_path = "/home/muditha/python/d03_RAINNC.nc"
bucket_output_dir = "wrf_nfs/wrf/{}/{}/{}/{}/output/mwrf/{}/".format(wrf_version, gfs_run, gfs_hour, output_date, model)

os.system("gsutil cp {} gs://{}".format(local_d03_RAINNC_file_path, bucket_output_dir))
