
import iris
import os
import sys
import glob
import numpy as np
import datetime
import matplotlib.pyplot as plt
import xarray as xr
import gzip
import dask
import shutil
import argparse
import yaml

from met_functions import *

"""
## 1. Parse arguments and set up
"""

print("starting joining script") 
parser = argparse.ArgumentParser(description='get big met')
parser.add_argument('year', metavar='y', type=int, nargs='+',
                    help='year to process')
parser.add_argument('month', metavar='m', type=int, help='month to process eg "00" for January')
parser.add_argument('regions', metavar='r', help='regions to process eg "NA" for North Africa')
parser.add_argument('--delete_files', default=False)
args = parser.parse_args()

# reference footprint
#fp = "/home/users/elenafi/satellite_met_scripts/GOSAT-BRAZIL-column_SOUTHAMERICA_201801.nc"

# Load yaml and extract relevant details for the domain of interest
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

    
region_key = args.regions
domain_info = config["domains"].get(region_key)
footprint_base = config.get("reference_footprints_directory", "")

if domain_info is None:
    raise ValueError(f"Unknown region key: {region_key}")

regions = domain_info["world_regions_codes"]

#fp = xr.open_dataset(fp)
region_key = args.regions
domain = config["domains"].get(region_key)["domain_name"]
print("getting args and setting up")
# define start and end date (month by month)
all_months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
month = all_months[args.month]
year = args.year[0]
days_in_month = np.array([ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
date = str(year)+month  +"01" # use this when debugging only with one day
start_date = np.datetime64(str(year) + "-" + month + "-01")
end_date = start_date  + np.timedelta64(days_in_month[args.month], 'D')
#end_date = start_date  #use this when debugging only with one day
#fp = fp.sel(time=slice(start_date, end_date))
print("merging regional data for the period "+str(start_date) + " - " + str(end_date))


# get region files and how they are connected from notebook
# if connected by "longitude", they are side-by-side, if connected by "latitude" they are on top of each other
# NOTE this code assumes a square arrangement (2x2 regions)!

if domain == "SOUTHAMERICA":

    region_pairs = {(9, 10): 'not_connected',
    (9, 13): 'latitude',
    (9, 6): 'longitude',
    (10, 13): 'longitude',
    (10, 6): 'latitude',
    (13, 6): 'not_connected'}

if domain == "NORTHAFRICA":

    region_pairs = {(2, 7): 'not_connected',
    (2, 6): 'latitude',
    (6, 7): 'longitude',
    (2, 3): 'longitude',
    (3, 7): 'latitude',
    (6, 3): 'not_connected'}   

if domain == "CHINA":
    region_pairs = {(3, 8): 'not_connected',
    (3, 7): 'latitude',
    (3, 4): 'longitude',
    (4, 8): 'longitude',
    (7, 8): 'latitude',
    (7, 4): 'not_connected'}   

if domain == "INDIA":
    region_pairs = {
    (3, 7): 'latitude',
}  

homefolder = config.get("scratch_path", "")
homefolder = os.path.join(homefolder, "files/")
# check that all files to join have been created
files = glob.glob(homefolder+domain+"_Met_"+str(year)+month+"_*")
for reg in regions:
    if (np.sum(["_"+str(reg)+".nc" in f for f in files])) != 1:
        print("not all necessary files exist at homefolder ", homefolder)
        print("missing file for region ", str(reg))
        sys.exit()
print("all necessary region files exist")

region_bounds = get_saved_region_bounds()
 
# UM world regions
# figure out how to deal with 1 and 14 
region_grid = [
    [2, 3, 4, 5],
    [6, 7, 8, 9],
    [10, 11, 12, 13]
]
 
with dask.config.set(**{'array.slicing.split_large_chunks': True}):
    lat_arrays = []
    for lat_pair in [k for k,v in region_pairs.items() if v == "latitude"]:
        with  xr.open_dataset(homefolder+domain+"_Met_"+str(year)+month+"_"+str(lat_pair[0])+".nc") as var1:
            with xr.open_dataset(homefolder+domain+"_Met_"+str(year)+month+"_"+str(lat_pair[1])+".nc") as var2:
                print("loaded both datasets, merging")
                var1 = var1.sel(latitude=slice(region_bounds[lat_pair[0]][0], region_bounds[lat_pair[0]][1]), longitude=slice(region_bounds[lat_pair[0]][2], region_bounds[lat_pair[0]][3]))
                var2 = var2.sel(latitude=slice(region_bounds[lat_pair[1]][0], region_bounds[lat_pair[1]][1]), longitude=slice(region_bounds[lat_pair[1]][2], region_bounds[lat_pair[1]][3]))
                print("Var1 longitude duplicates:", var1.longitude.to_series().duplicated().any())
                print("Var2 longitude duplicates:", var2.longitude.to_series().duplicated().any())
                var1 = var1.isel(longitude=~var1.longitude.to_series().duplicated())
                var2 = var2.isel(longitude=~var2.longitude.to_series().duplicated())
                print("Var1 longitude duplicates:", var1.longitude.to_series().duplicated().any())
                print("Var2 longitude duplicates:", var2.longitude.to_series().duplicated().any())
                merged = xr.concat([var1, var2], dim="latitude")
                merged = merged.sortby("latitude")
                merged = merged.drop_duplicates(dim="latitude")
                lat_arrays.append(merged)
                print(lat_pair, "merged")
                print(merged)
                print(np.sum(np.isnan(merged.x_wind[0,:,:,0].values)))
                
        """ 
        var1 = xr.open_dataset(homefolder+domain+"_Met_"+str(year)+month+"_"+str(lat_pair[0])+".nc")
        var2 = xr.open_dataset(homefolder+domain+"_Met_"+str(year)+month+"_"+str(lat_pair[1])+".nc")    
        print("loaded both datasets, merging")
        merged = xr.concat([var1, var2], dim="latitude")
        merged = merged.drop_duplicates(dim="latitude")
        lat_arrays.append(merged)
        print(lat_pair, "merged")
        """

    print("fixing attrs")
    met = xr.concat(lat_arrays, dim="longitude")
    met = met.drop_duplicates(dim="longitude")
    met = met.sortby(["latitude", "longitude", "time"])
    met = met.transpose("model_level_number", "latitude", "longitude", "time")
    print(np.sum(np.isnan(met.x_wind[0,:,:,0].values)))
    for attr in ["units", "standard_name", "STASH"]:
      try:
        met.attrs.pop(attr)
      except:
        print(f"no attr {attr}")
        continue
    met.attrs["author"] = config.get("met_extract_author", "")
    met.attrs["created"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    met.attrs["transformations"] = "interpolated linearly in space to NAME resolution"#, interpolated linearly in time from 3-hourly to hourly"
    print("at the end", met)
    
    filename = config.get("met_save_directory", "")+domain+"_Met_"+str(year)+month+".nc"
    print("saving", filename)

    met.load().to_netcdf(filename) 
    file_stats = os.stat(filename)
    print(f'Saved! File Size in MegaBytes is {file_stats.st_size / (1024 * 1024)}')

if args.delete_files:
    os.system("rm -r " + homefolder+domain+"_Met_"+str(year)+month+"_*")
            
