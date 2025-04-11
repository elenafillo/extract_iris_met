
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


def get_default_paths():
    # Moving all hard-coded locations to config.yaml.
    # To remove this function as soon as I've finished with satellite_met_join_v2.py
    stratch = "/work/scratch-nopw2/jeff/"

    paths = {"stratch":stratch}

    return paths

def get_saved_region_bounds(lon_format="default"):
    """
    Get the bounds of each world region
    the longitude line change is treated differently depending on the goal of 
    extracting the saved regions
    - 180: longitude in range -180 to 180 
    - plot: for plotting map only
    - extracting: set up so that extracting works
    - as_in_cube: longitude in range 0-405, wrapping around the earth (as present exactly in the cube data). removes instability in the edge and zero regions but its odd

    """

    valid_lon_formats = ["default", "180", "plot", "360"]
    if lon_format not in valid_lon_formats:
        print(f"you passed {lon_format} but it isn't one of the valid longitude formats {valid_lon_formats}! Returning -180 to 180 as default")
        lon_format = "default"

    if lon_format=="180" or lon_format=="default":
        region_bounds =    {1: [79.921875, 89.953125, 0.0703125, -0.0703125],
            2: [24.984375, 80.015625, -45.070312, 45.070312],
            3: [24.984375, 80.015625, 44.929688, 135.07031],
            4: [24.984375, 80.015625, 134.92969, -134.92969],
            5: [24.984375, 80.015625, -135.07031, -44.929688],
            6: [-25.078125, 25.078125, -45.070312, 45.070312],
            7: [-25.078125, 25.078125, 44.929688, 135.07031],
            8: [-25.078125, 25.078125, 134.92969, -134.92969],
            9: [-25.078125, 25.078125, -135.07031, -44.929688],
            10: [-80.015625, -24.984375, -45.070312, 45.070312],
            11: [-80.015625, -24.984375, 44.929688, 135.07031],
            12: [-80.015625, -24.984375, 134.92969, -134.92969],
            13: [-80.015625, -24.984375, -135.07031, -44.929688],
            14: [-89.953125, -79.921875, 0.0703125, -0.0703125],
            }
    if lon_format=="plot":
        region_bounds = {1: [79.921875, 89.953125, -179.92969, 179.92969],
            2: [24.984375, 80.015625, -45.070312, 45.070312],
            3: [24.984375, 80.015625, 44.929688, 135.07031],
            4: [24.984375, 80.015625, -179.92969, 179.92969],
            5: [24.984375, 80.015625, -135.07031, -44.929688],
            6: [-25.078125, 25.078125, -45.070312, 45.070312],
            7: [-25.078125, 25.078125, 44.929688, 135.07031],
            8: [-25.078125, 25.078125, -179.92969, 179.92969],
            9: [-25.078125, 25.078125, -135.07031, -44.929688],
            10: [-80.015625, -24.984375, -45.070312, 45.070312],
            11: [-80.015625, -24.984375, 44.929688, 135.07031],
            12: [-80.015625, -24.984375, -179.92969, 179.92969],
            13: [-80.015625, -24.984375, -135.07031, -44.929688],
            14: [-89.953125, -79.921875, -179.92969, 179.92969]}
    if lon_format == "as_in_cube":
        region_bounds = {1: [79.921875, 89.953125, 0.070, 359.9297],
            2: [24.984375, 80.015625, 314.929, 405.070],
            3: [24.984375, 80.015625, 44.929688, 135.07031],
            4: [24.984375, 80.015625, 134.92969, 225.07031],
            5: [24.984375, 80.015625, 224.92969, 315.0703],
            6: [-25.078125, 25.078125, 314.9297, 405.0703],
            7: [-25.078125, 25.078125, 44.929688, 135.07031],
            8: [-25.078125, 25.078125, 134.92969, 225.07031],
            9: [-25.078125, 25.078125, 224.92969, 315.0703],
            10: [-80.015625, -24.984375, 314.9297, 405.0703],
            11: [-80.015625, -24.984375, 44.929688, 135.07031],
            12: [-80.015625, -24.984375, 134.92969, 225.07031],
            13: [-80.015625, -24.984375, 224.92969, 315.0703],
            14: [-89.953125, -79.921875, 0.0703125, 359.9297]}
    if lon_format == "extracting":
        region_bounds =    {1: [79.921875, 89.953125, 0.070, 359.9297],
            2: [24.984375, 80.015625, -45.070312, 45.070312],
            3: [24.984375, 80.015625, 44.929688, 135.07031],
            4: [24.984375, 80.015625, 134.92969, 225.07031],
            5: [24.984375, 80.015625, -135.07031, -44.929688],
            6: [-25.078125, 25.078125, -45.070312, 45.070312],
            7: [-25.078125, 25.078125, 44.929688, 135.07031],
            8: [-25.078125, 25.078125, 134.92969, 225.07031],
            9: [-25.078125, 25.078125, -135.07031, -44.929688],
            10: [-80.015625, -24.984375, -45.070312, 45.070312],
            11: [-80.015625, -24.984375, 44.929688, 135.07031],
            12: [-80.015625, -24.984375, 134.92969, 225.07031],
            13: [-80.015625, -24.984375, -135.07031, -44.929688],
            14: [-89.953125, -79.921875, 0.0703125, 359.9297],
            }
    return region_bounds

def find_overlapping_regions(min_lat, max_lat, min_lon, max_lon):
    """
    Given a bounding box (min/max latitude and longitude), return the region IDs that overlap.


    Returns:
    list: Region IDs that overlap with the given domain
    """
    region_bounds = get_saved_region_bounds()
    overlapping_regions = []

    for region_id, (r_min_lat, r_max_lat, r_min_lon, r_max_lon) in region_bounds.items():
        # Check if the bounding boxes overlap
        lat_overlap = not (max_lat < r_min_lat or min_lat > r_max_lat)
        lon_overlap = not (max_lon < r_min_lon or min_lon > r_max_lon)

        if lat_overlap and lon_overlap:
            overlapping_regions.append(region_id)

    return overlapping_regions


def load_iris(filepath, Mk, date, vars, num, homefolder):
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

        # Load config.yaml to save to updates.txt
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
        scripts_text = config.get("scripts_text_save_location", "")
        if len(homefiles)==len(files):
            txtfile = open(scripts_text, "a+")
            txtfile.write(date + str(num) + "  " + str(datetime.datetime.now()) + " --- loading data from scratch --- \n")
            txtfile.close()   
            print("needed files are already in homefolder. Loading without copying")
            bad_files = [homefolder+f for f in bad_files]
            homefiles = list(set(homefiles) - set(bad_files))
            if len(homefiles) < nhomefiles:
                print(str(nhomefiles-len(homefiles)), " files removed")
            try:
                print("attempt loading")
                loaded = iris.load(homefiles, vars, callback=remove_coord_callback)
                txtfile = open(scripts_text, "a")
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

def daterange(start_date, end_date, a_day_only=False):
    start_date = np.datetime64(start_date)
    if a_day_only:
        end_date = np.datetime64(start_date) + np.timedelta64(24, 'h') # use this when debugging only with one day
    else:
        end_date = np.datetime64(end_date)
    dates = []
    datenow = start_date + np.timedelta64(0, 'h')
    while datenow < end_date:
        dates.append(np.datetime64(datenow))
        datenow = datenow + np.timedelta64(1, 'h')
    return np.array(dates)

def get_Mk(year, month):
    # find file naming convention for date
    if year==2011 or year==2012 or (year == 2013 and month in ["01", "02", "03", "04"]):
        Mk = 6
    elif (year == 2013 and month in ["05", "06", "07","08", "09", "10", "11", "12"]) or (year == 2014 and month in ["01", "02", "03", "04", "05", "06"]):
        Mk = 7
    elif (year == 2014 and month in ["07", "08", "09", "10", "11", "12"]) or (year == 2015 and month in ["01", "02", "03", "04", "05", "06", "07"]):
        Mk = 8
    elif (year == 2015 and month in ["08", "09", "10", "11", "12"]) or (year == 2016) or (year == 2017 and month in ["01", "02", "03", "04", "05", "06"]):
        Mk = 9
    elif (year == 2017 and month in ["07", "08", "09", "10", "11", "12"]) or (year > 2017 and (year < 2022 or (year == 2022 and month in ["01", "02", "03", "04", "05"]))):
        Mk = 10
    elif (year == 2022 and month in ["06", "07", "08", "09", "10", "11", "12"]) or (year > 2022):
        Mk = 11
    else:
        print("No Mk found for this year and month")
        raise ValueError(f"No Mk version found for year={year}, month={month}")
    return Mk

def get_edge_size(domain, size_type):

    # Retrieve the domain or global default edge size from the yaml configuration file
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    default_edge_size = config.get('default_edge_size', [100, 100])

    try:
        return config['domains'][domain].get(size_type, default_edge_size)
    except KeyError:
        return default_edge_size