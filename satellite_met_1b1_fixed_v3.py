
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


print("getting some levels of met")


parser = argparse.ArgumentParser(description='get big met')
parser.add_argument('year', metavar='y', type=int, nargs='+',
                    help='year to process')
parser.add_argument('month', metavar='m', type=int, help='month to process')
parser.add_argument('regions', metavar='r', help='regions to process')
args = parser.parse_args()

a_day_only = False

homefolder = "/work/scratch-nopw2/elenafi/"


def load_iris(filepath, Mk, date, vars, num, homefolder):
    print("In load iris")
    bad_files = ["MO201402011500.UMG_Mk7_I_L59PT9.pp"] 
    # mk10 files are already unzipped, can load directly - changed, trying this
    if Mk == 10:
        filename = filepath[0]+date+filepath[1]+ str(num) + ".pp"
        print(filename, len(glob.glob(filename)))
        try:
            # load
            loaded = iris.load(glob.glob(filename), vars, callback=remove_coord_callback)

        except Exception as e:
            # delete all files if something fails
            print("something failed: ", e )
            os.system("rm -r " + homefolder + "MO"+date+"*")     
    # rest of mks needs to be unzipped in a different directory
    else:
        filename = filepath[0]+date+filepath[1]+ str(num) + ".pp.gz"
        files = glob.glob(filename)
        print(filename, len(glob.glob(filename)))
        print(homefolder+date+filepath[1]+ str(num) + ".pp")
        homefiles = glob.glob(homefolder+"*"+date+filepath[1]+ str(num) + ".pp")
        nhomefiles = len(homefiles)
        #print(list(set([os.path.basename(f).replace(".gz", "") for f in files]) - set([os.path.basename(f) for f in homefiles])))
        print(len(homefiles), len(files))
        if len(homefiles)==len(files):
            txtfile = open("/home/users/elenafi/satellite_met_scripts/updates.txt", "a")
            txtfile.write(date + str(num) + "  " + str(datetime.datetime.now()) + "loading data from scratch \n")
            txtfile.close()   
            print("needed files are already in homefolder. Loading without copying")
            bad_files = [homefolder+f for f in bad_files]
            homefiles = list(set(homefiles) - set(bad_files))
            if len(homefiles) < nhomefiles:
                print(str(nhomefiles-len(homefiles)), " files removed")
            try:
                loaded = iris.load(homefiles, vars, callback=remove_coord_callback)
                txtfile = open("/home/users/elenafi/satellite_met_scripts/updates.txt", "a")
                txtfile.write(date + str(num) + "  " + str(datetime.datetime.now()) + " data loaded (directly from homefolder)\n")
                txtfile.close()  
            except Exception as e:
                print("something failed: ", e )
        try:
            all_outs = []
            for f in files:
                filename = os.path.basename(f)
                filename = filename.replace(".gz", "")
                # unzip and copy to scratch
                with gzip.open(f, 'rb') as f_in:
                    with open(homefolder+filename, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                        if filename not in bad_files:
                            all_outs.append(homefolder+filename)
            print("all copied")
            # load
            loaded = iris.load(all_outs, vars, callback=remove_coord_callback)
            print("loaded")
        except Exception as e:
            # delete all files if something fails
            print("something failed: ", e )
            #os.system("rm -r " + homefolder + "MO"+date+"*")
            
    return loaded

def delete_iris(homefolder, date, num):
    os.system("rm -r " + homefolder + "MO"+date+"*"+ str(num) + ".pp")

def remove_coord_callback(cube, field, filename):
    if "um_version" in cube.attributes.keys():
        cube.attributes.pop("um_version")

def daterange(start_date, end_date):
    start_date = np.datetime64(start_date)
    if a_day_only:
        end_date = np.datetime64(end_date) + np.timedelta64(24, 'h') # use this when debugging only with one day
    else:
        end_date = np.datetime64(end_date)
    dates = []
    datenow = start_date + np.timedelta64(0, 'h')
    while datenow < end_date:
        dates.append(np.datetime64(datenow))
        datenow = datenow + np.timedelta64(1, 'h')
    return np.array(dates)
        
regions = args.regions
print(regions)
if regions == "all":
    regions = [9, 10, 13, 6]
if regions == "NA":
    regions = [6,2,3,7]
#regions = [6,10]
    
# NorthAfrica regions: [2,6] if using only obs, [2,6,3,7] if using wider edge

# reference footprint
fp = "/home/users/elenafi/satellite_met_scripts/GOSAT-BRAZIL-column_SOUTHAMERICA_201801.nc"
fp = "/home/users/elenafi/satellite_met_scripts/GOSAT-SAHARA-column_NORTHAFRICA_201611.nc"

fp = xr.load_dataset(fp)
domain = "NORTHAFRICA"
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
    end_date = start_date  + np.timedelta64(10, 'D')
#fp = fp.sel(time=slice(start_date, end_date))
print("getting met for the period "+str(start_date) + " - " + str(end_date))


latitudes = list(fp.lat.values)
longitudes = list(fp.lon.values)

print("adding extra longitudes to reference grid")

print(fp)

delta_lon = 0.352
delta_lat = 0.234

#longitudes = np.array(sorted(longitudes + [np.min(longitudes)-delta_lon*i for i in range(50)] + [np.max(longitudes)+delta_lon*i for i in range(50)]))
longitudes = np.array(sorted(longitudes + [np.max(longitudes)+delta_lon*i for i in range(70)] + [np.min(longitudes)-delta_lon*i for i in range(20)]))
#longitudes = np.array(sorted([np.max(longitudes)+delta_lon*i for i in range(50)]))

latitudes = np.array(fp.lat.values)


## another way to do it - select around the min/max locations of measurements
fp = fp.sel(lon=slice(np.min(fp.release_lon), np.max(fp.release_lon)), lat=slice(np.min(fp.release_lat), np.max(fp.release_lat)))

edge_size_lat = [100,100]
edge_size_lon = [85, 100]

latitudes = list(fp.lat.values)
longitudes = list(fp.lon.values)


longitudes = np.array(sorted(longitudes + [np.max(longitudes)+delta_lon*i for i in range(edge_size_lon[1])] + [np.min(longitudes)-delta_lon*i for i in range(edge_size_lon[0])]))

latitudes = np.array(sorted(latitudes + [np.max(latitudes)+delta_lat*i for i in range(edge_size_lat[1])] + [np.min(latitudes)-delta_lat*i for i in range(edge_size_lat[0])]))

fp.close()


# find file naming convention for date
if year==2011 or year==2012 or (year == 2013 and month in ["01", "02", "03", "04"]):
    Mk = 6
elif (year == 2014 and month in ["01", "02", "03", "04", "05", "06"]) or (year == 2013 and month in ["05", "06", "07","08", "09", "10", "11", "12"]):
    Mk = 7
elif (year == 2014 and month in ["07", "08", "09", "10", "11", "12"]) or (year == 2015 and month in ["01", "02", "03", "04", "05", "06", "07"]):
    Mk = 8
elif (year == 2015 and month in ["08", "09", "10", "11", "12"]) or (year == 2016) or (year == 2017 and month in ["01", "02", "03", "04", "05", "06"]):
    Mk = 9
elif (year == 2017 and month in ["07", "08", "09", "10", "11", "12"]) or (year > 2017):
    Mk = 10
else:
    print("No Mk found for this year and month")

## Mk11 - 2022 06 onwards
    
    
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

region_bounds = {1: [79.921875, 89.953125, -179.92969, 179.92969],
 10: [-80.015625, -24.984375, -45.070312, 45.070312],
 11: [-80.015625, -24.984375, 44.929688, 135.07031],
 12: [-80.015625, -24.984375, -179.92969, 179.92969],
 13: [-80.015625, -24.984375, -135.07031, -44.929688],
 14: [-89.953125, -79.921875, -179.92969, 179.92969],
 2: [24.984375, 80.015625, -45.070312, 45.070312],
 3: [24.984375, 80.015625, 44.929688, 135.07031],
 4: [24.984375, 80.015625, -179.92969, 179.92969],
 5: [24.984375, 80.015625, -135.07031, -44.929688],
 6: [-25.078125, 25.078125, -45.070312, 45.070312],
 7: [-25.078125, 25.078125, 44.929688, 135.07031],
 8: [-25.078125, 25.078125, -179.92969, 179.92969],
 9: [-25.078125, 25.078125, -135.07031, -44.929688]}
 
 

with dask.config.set(**{'array.slicing.split_large_chunks': True}):
    for reg in regions:
        print(f"Doing region {reg}")
        try:
            cube = load_iris(filepath, Mk, date, vars, reg, homefolder)
        except:
            print("skipping region", reg)
            continue
        print(reg, "loaded")

        txtfile = open("/home/users/elenafi/satellite_met_scripts/updates.txt", "a")
        txtfile.write(date + str(reg) + "  " +str(datetime.datetime.now()) + "starting \n")
        txtfile.close()  
        
        
        most_variables = xr.combine_by_coords([xr.DataArray.from_iris(cube[i]) for i in [0,1,3,4,5,7]]).sel(model_level_number=levels)
        x_wind = xr.combine_by_coords([xr.DataArray.from_iris(cube[8])], compat="override").sel(model_level_number=levels)
        y_wind = xr.combine_by_coords([xr.DataArray.from_iris(cube[9])], compat="override").sel(model_level_number=levels)
        
        del cube 
        
        all_variables = xr.combine_by_coords([most_variables, x_wind.interp(latitude=most_variables.latitude.values, longitude=most_variables.longitude.values), y_wind.interp(latitude=most_variables.latitude.values, longitude=most_variables.longitude.values)], compat="override")
        
        del most_variables, x_wind, y_wind
        
        all_variables.load()
        

        txtfile = open("/home/users/elenafi/satellite_met_scripts/updates.txt", "a")
        txtfile.write(date + str(reg) + "  " +str(datetime.datetime.now()) + "dataset created successfully \n")
        txtfile.close()  
        
        all_variables = all_variables.assign_coords(longitude=(((all_variables.longitude + 180) % 360) - 180))

        print(all_variables)

        if "time" not in list(all_variables.sizes.keys()):
            print("stacking time variables")
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
        print("Interpolated!!")
        print(interpolated)
            
        txtfile = open("/home/users/elenafi/satellite_met_scripts/updates.txt", "a")
        txtfile.write(date + str(reg) + "  " + str(datetime.datetime.now()) + "interpolated! chunking \n")
        txtfile.write(str(len(interpolated.longitude)) + "\n")
        txtfile.close()  
        
        interpolated = interpolated.chunk("auto")
        
        print(interpolated)
            
        txtfile = open("/home/users/elenafi/satellite_met_scripts/updates.txt", "a")
        txtfile.write(date + str(reg) + "  " + str(datetime.datetime.now()) + "chunked! saving \n")
        txtfile.write(str(len(interpolated.longitude)) + "\n")
        txtfile.close()  
        
        #interpolated.load()

        filename = homefolder+"files/"+domain+"_Met_"+str(year)+month+"_"+str(reg)+".nc"
        interpolated.to_netcdf(filename)            
 
        txtfile = open("/home/users/elenafi/satellite_met_scripts/updates.txt", "a")
        txtfile.write(date + str(reg) + "  " + str(datetime.datetime.now()) + f"saved successfully at {filename} \n")
        txtfile.close()  

        del interpolated
        del all_variables
    







