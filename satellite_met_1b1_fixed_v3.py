
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

from met_functions import *
import yaml
import traceback




print("getting some levels of met")

"""
## 1. Parse arguments and set up
"""

parser = argparse.ArgumentParser(description='get big met')
parser.add_argument('year', metavar='y', type=int, nargs='+',
                    help='year to process')
parser.add_argument('month', metavar='m', type=int, help='month to process eg "00" for January')
parser.add_argument('regions', metavar='r', help='regions to process eg "NA" for North Africa')
args = parser.parse_args()

# a_day_only runs a debugging single day just to check it all works, but can probs be removed
a_day_only = False

# Load yaml and extract relevant details for the domain of interest
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)


homefolder = config.get("scratch_path", "")

files_dir = os.path.join(homefolder, "files")
# Create the 'files' directory if it doesn't exist
if not os.path.exists(files_dir):
    os.makedirs(files_dir, exist_ok=True)

region_key = args.regions
domain_info = config["domains"].get(region_key)
footprint_base = config.get("reference_footprints_directory", "")

if domain_info is None:
    raise ValueError(f"Unknown region key: {region_key}")

regions = domain_info["world_regions_codes"]
fp = os.path.join(footprint_base, domain_info["footprint"])

# Check there is actually a suitable reference footprint
if not os.path.exists(fp):
    raise FileNotFoundError(f"Reference footprint file does not exist: {fp}")

domain_name = domain_info["domain_name"]

# Define where to save 
scripts_text = config.get("scripts_text_save_location", "")
# Create the folder if it doesn't exist
folder = os.path.dirname(scripts_text)
if folder:  # only try to make directory if folder part is not empty
    os.makedirs(folder, exist_ok=True)

print(f"--- running for {domain_name}, {args.year[0]} {args.month}, a_day_only {a_day_only} ---")

print("getting args and setting up")
# define start and end date (month by month)
all_months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
month = all_months[args.month]
year = args.year[0]
days_in_month = np.array([ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
date = str(year)+month
start_date = np.datetime64(str(year) + "-" + month + "-01")
end_date = start_date  + np.timedelta64(days_in_month[args.month], 'D')
if a_day_only:
    date = str(year)+month  +"1" # use this when debugging only with one day
    #end_date = start_date  #use this when debugging only with one day
    end_date = start_date  + np.timedelta64(24, 'h')

print("getting met for the period "+str(start_date) + " - " + str(end_date))

"""
## 2. Select the latitudes and longitudes to be extracted
At the moment, the way this is done is by loading a reference footprint for the domain and taking its latitude and longitude.
To make the domain larger than this, there are the 
edge_size_lat and edge_size_lon parameters, which indicate how many latitude and longitude gridcells need to be added in each direction eg
    # edge_size_lat  = [extra gridcells to the south, extra gridcells to the north]
    # edge_size_lon  = [extra gridcells to the west, extra gridcells to the east]
    edge_size_lat = [100,100] 
    edge_size_lon = [85, 100]

"""

fp = xr.load_dataset(fp)

latitudes = list(fp.lat.values)
longitudes = list(fp.lon.values)

print("adding extra longitudes to reference grid")



delta_lon = 0.352
delta_lat = 0.234

#longitudes = np.array(sorted(longitudes + [np.min(longitudes)-delta_lon*i for i in range(50)] + [np.max(longitudes)+delta_lon*i for i in range(50)]))
longitudes = np.array(sorted(longitudes + [np.max(longitudes)+delta_lon*i for i in range(70)] + [np.min(longitudes)-delta_lon*i for i in range(20)]))
#longitudes = np.array(sorted([np.max(longitudes)+delta_lon*i for i in range(50)]))

latitudes = np.array(fp.lat.values)


## another way to do it - select around the min/max locations of measurements
fp = fp.sel(lon=slice(np.min(fp.release_lon), np.max(fp.release_lon)), lat=slice(np.min(fp.release_lat), np.max(fp.release_lat)))



edge_size_lat = get_edge_size(region_key, 'edge_size_lat')
edge_size_lon = get_edge_size(region_key, 'edge_size_lon')

print(f"Edge size for {region_key} - Latitude: {edge_size_lat}, Longitude: {edge_size_lon}")

latitudes = list(fp.lat.values)
longitudes = list(fp.lon.values)


longitudes = np.array(sorted(longitudes + [np.max(longitudes)+delta_lon*i for i in range(edge_size_lon[1])] + [np.min(longitudes)-delta_lon*i for i in range(edge_size_lon[0])]))

latitudes = np.array(sorted(latitudes + [np.max(latitudes)+delta_lat*i for i in range(edge_size_lat[1])] + [np.min(latitudes)-delta_lat*i for i in range(edge_size_lat[0])]))

fp.close()

"""
## 3.  Set up the parameters of the meteorology files
"""
Mk = get_Mk(year, month)
    
if Mk == 6:
    filepath =  ["/gws/nopw/j04/name/met_archive/Global/UMG_Mk"+str(Mk)+"PT/MO", "*.UMG_Mk"+str(Mk)+"_L59PT"]
elif Mk != 10:
    filepath =  ["/gws/nopw/j04/name/met_archive/Global/UMG_Mk"+str(Mk)+"PT/MO", "*.UMG_Mk"+str(Mk)+"_I_L59PT"]
if Mk == 10:
    filepath =  ["/gws/nopw/j04/name/met_archive/Global/UMG_Mk"+str(Mk)+"PT/MO","*.UMG_Mk"+str(Mk)+"_I_L59PT"]


# get region files and how they are connected from notebook
# NOTE this code assumes a square arrangement (2x2 regions)!
#regions = [9, 10, 13, 6]
#regions = [9]

## load only one in every three levels (as well as surface) to reduce memory usage
levels = [1]+list(range(1,60))[2::3]

## note that air_temperature loads twice because theres a var with heights and one without
## so actual vars loaded are len(vars)+1
vars = ["air_pressure", "air_pressure_at_sea_level", "air_temperature", "atmosphere_boundary_layer_thickness", "surface_air_pressure", "surface_upward_sensible_heat_flux", "upward_air_velocity", "x_wind", "y_wind"]

region_bounds = get_saved_region_bounds()
 

"""
## 4.  Extract the met for each region

the load_iris opens the .pp files, copying them to scratch and unzipping them if necessary

"""

print("**** Now ready to do main loop ****")
with dask.config.set(**{'array.slicing.split_large_chunks': True}):
    for reg in regions:
        print(f"Processing region {reg}")
        try:
            cube = load_iris(filepath, Mk, date, vars, reg, homefolder)
            print(f"region {reg} loaded")

            txtfile = open(scripts_text, "a")
            txtfile.write(date + str(reg) + "  " +str(datetime.datetime.now()) + f"starting for {domain_name}, {args.year[0]} {args.month}, a_day_only {a_day_only}, region {reg}\n")
            txtfile.close()  
        
            # some variables have slightly different coord systems for some reason?
            most_variables = xr.combine_by_coords([xr.DataArray.from_iris(cube[i]) for i in [0,1,3,4,5,7]]).sel(model_level_number=levels)
            x_wind = xr.combine_by_coords([xr.DataArray.from_iris(cube[8])], compat="override").sel(model_level_number=levels)
            y_wind = xr.combine_by_coords([xr.DataArray.from_iris(cube[9])], compat="override").sel(model_level_number=levels)
            del cube 
            
            all_variables = xr.combine_by_coords([
                most_variables,
                x_wind.interp(latitude=most_variables.latitude.values, longitude=most_variables.longitude.values),
                y_wind.interp(latitude=most_variables.latitude.values, longitude=most_variables.longitude.values)
            ], compat="override")
            
            del most_variables, x_wind, y_wind
            
            all_variables.load()
            

            txtfile = open(scripts_text, "a")
            txtfile.write(date + str(reg) + "  " +str(datetime.datetime.now()) + "dataset created successfully \n")
            txtfile.close()  
            
            all_variables = all_variables.assign_coords(longitude=(((all_variables.longitude + 180) % 360) - 180))

            print(all_variables)

            if "time" not in list(all_variables.sizes.keys()):
                all_variables = all_variables.stack(newtime = ["forecast_period", "forecast_reference_time"])
                
                all_variables = all_variables.swap_dims({"newtime":"time"})
                
                all_variables = all_variables.drop_vars(['forecast_period', "forecast_reference_time", "newtime"])
            else:
                all_variables = all_variables.transpose("model_level_number", "latitude", "longitude", "time", ...)

            # interpolating to correct resolution, then slicing back to region domain
            all_variables = all_variables.interp(latitude=latitudes,longitude=longitudes)

            all_variables = all_variables.sel(latitude=slice(region_bounds[reg][0], region_bounds[reg][1]), longitude=slice(region_bounds[reg][2], region_bounds[reg][3]))
            
            
            interpolated = all_variables.sortby("time")
            # NOT INTERPOLATING HOURLY???
            #interpolated = interpolated.resample(time="1h").interpolate("linear")
            print("interpolation complete")
            print(interpolated)
                
            txtfile = open(scripts_text, "a")
            txtfile.write(date + str(reg) + "  " + str(datetime.datetime.now()) + "interpolated! chunking \n")
            txtfile.write(str(len(interpolated.longitude)) + "\n")
            txtfile.close()  
            
            interpolated = interpolated.chunk("auto")
            
            print(interpolated)
                
            txtfile = open(scripts_text, "a")
            txtfile.write(date + str(reg) + "  " + str(datetime.datetime.now()) + "chunked! saving \n")
            txtfile.write(str(len(interpolated.longitude)) + "\n")
            txtfile.close()  
            
            #interpolated.load()

            filename = homefolder+"files/"+domain_name+"_Met_"+str(year)+month+"_"+str(reg)+".nc"
            
            print(interpolated.dims)
            print(interpolated.coords)
            print(interpolated.data_vars)

            print("saving...")
            interpolated.to_netcdf(filename)            

            print(f"saved as {filename}", flush=True)
            txtfile = open(scripts_text, "a")
            txtfile.write(date + str(reg) + "  " + str(datetime.datetime.now()) + f"saved successfully at {filename} \n")
            txtfile.close()  

            print(f"---- Finished region {reg} ----")

            del interpolated
            del all_variables
        except Exception as e:
            print(f"Skipping region {reg} due to error: {e}", flush=True)
            traceback.print_exc()
            with open(scripts_text, "a") as txtfile:
                txtfile.write(f"{date}{reg}  {datetime.datetime.now()} error: {e}\n")
            continue

    print("---- All processing complete ---")
print("----- Script finished successfully -----")






