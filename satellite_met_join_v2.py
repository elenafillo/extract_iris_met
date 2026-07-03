
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

"""
## 1. Parse arguments and set up
"""
DEBUG_DUPLICATES = False
COORD_DECIMALS = 6


def log(msg):
    """Print message with timestamp."""
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def normalize_coords(ds, decimals=COORD_DECIMALS):
    """Round lat/lon coordinates to avoid tiny cross-region precision mismatches."""
    return ds.assign_coords(
        latitude=np.round(ds.latitude.values.astype(np.float64), decimals).astype(np.float32),
        longitude=np.round(ds.longitude.values.astype(np.float64), decimals).astype(np.float32),
    )


def fill_join_seams(ds):
    """Fill 1-cell NaN seams introduced at region boundaries during stitching."""
    ds = ds.interpolate_na(dim="latitude", method="linear", limit=1)
    ds = ds.interpolate_na(dim="longitude", method="linear", limit=1)
    ds = ds.interpolate_na(dim="latitude", method="nearest", limit=1)
    ds = ds.interpolate_na(dim="longitude", method="nearest", limit=1)
    return ds


log("starting joining script")
parser = argparse.ArgumentParser(description='get big met')
parser.add_argument('year', metavar='y', type=int, nargs='+',
                    help='year to process')
parser.add_argument('month', metavar='m', type=int, help='month to process eg "00" for January')
parser.add_argument('regions', metavar='r', help='regions to process eg "NA" for North Africa')
parser.add_argument('--delete_files', default=False)
args = parser.parse_args()

# reference footprint
#fp = "/home/users/elenafi/satellite_met_scripts/GOSAT-BRAZIL-column_SOUTHAMERICA_201801.nc"

# Load yaml and extract relevant details for the domain of interest
config = load_config()

    
region_key = args.regions
domain_info = config["domains"].get(region_key)
footprint_base = config.get("reference_footprints_directory", "")
footprint_base = resolve_config_value(footprint_base, config)

if domain_info is None:
    raise ValueError(f"Unknown region key: {region_key}")

regions = domain_info["world_regions_codes"]

#fp = xr.open_dataset(fp)
region_key = args.regions
domain = config["domains"].get(region_key)["domain_name"]
log("getting args and setting up")
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
log("merging regional data for the period "+str(start_date) + " - " + str(end_date))


# get region files and how they are connected from notebook
# if connected by "longitude", they are side-by-side, if connected by "latitude" they are on top of each other
# NOTE this code assumes a square arrangement (2x2 regions)!

# UM world regions
# figure out how to deal with 1 and 14 
global_region_grid = [
    [2, 3, 4, 5],
    [6, 7, 8, 9],
    [10, 11, 12, 13]
]

# Now make it domain-specifc
region_grid = build_domain_grid(global_region_grid, regions)

log(f"region grid: {region_grid}")


homefolder = resolve_config_value(config.get("scratch_path", ""), config)
homefolder = os.path.join(homefolder, "files/")
# check that all files to join have been created
files = glob.glob(homefolder+domain+"_Met_"+str(year)+month+"_*")
for reg in regions:
    if (np.sum(["_"+str(reg)+".nc" in f for f in files])) != 1:
        print("not all necessary files exist at homefolder ", homefolder)
        print("missing file for region ", str(reg))
        sys.exit()
log("all necessary region files exist")

region_bounds = get_saved_region_bounds()
 

 
with dask.config.set(**{'array.slicing.split_large_chunks': True}):
    log("Trying agnostic met joining")
    lat_arrays = []
    for column in zip(*region_grid):
        lat_datasets = []
        for region in column:
            file_path = f"{homefolder}{domain}_Met_{year}{month}_{region}.nc"

            log(f"Opening region {region}: {file_path}")
            ds = xr.open_dataset(file_path).astype("float32")
            region_size_mb = ds.nbytes / (1024 * 1024)
            log(f"Region {region} opened — lon range: {ds.longitude.min().values:.2f} to {ds.longitude.max().values:.2f}, lat range: {ds.latitude.min().values:.2f} to {ds.latitude.max().values:.2f}")
            log(f"Region {region} size after float32 cast: {region_size_mb:.1f} MB")
            '''
            # If near 180 degrees, adjust the longitude values
            if ds.longitude.min() < 0:
                print("Converting longitudes from [-180, 180] to [0, 360]")
                ds = ds.assign_coords(longitude=(ds.longitude % 360))
                ds = ds.sortby("longitude")
            print("Augmented coords: Min lon, max lon:", ds.longitude.min().values, ds.longitude.max().values)
            '''

            bounds = region_bounds[region]
            log(
                f"Region {region} nominal bounds: "
                f"lat [{bounds[0]}, {bounds[1]}], lon [{bounds[2]}, {bounds[3]}]"
            )
            ds = normalize_coords(ds)
            '''
            if ds.sizes["latitude"] == 0 or ds.sizes["longitude"] == 0:
                print(f" Skipping region {region}: slicing returned empty dataset")
                print(f" Slicing bounds: {bounds}")
                if ds.latitude.size > 0 and ds.longitude.size > 0:
                    print(f"  Dataset lat range: {ds.latitude.min().values} to {ds.latitude.max().values}")
                    print(f"  Dataset lon range: {ds.longitude.min().values} to {ds.longitude.max().values}")
                else:
                    print("  Dataset is completely empty (lat/lon coords not available)")
                continue
            '''
            # Drop duplicate longitudes and sort
            ds = ds.sortby("longitude")

            # Get longitude values
            longitudes = ds["longitude"].values

            # Count occurrences
            unique, counts = np.unique(longitudes, return_counts=True)

            # Find duplicates
            duplicates = unique[counts > 1]
            
            if duplicates.size > 0:
                log(f"Duplicate longitude values found in region {region}: {duplicates}")
                if DEBUG_DUPLICATES:
                    for dup in duplicates:
                        idxs = np.where(longitudes == dup)[0]
                        log(f"  Value {dup} appears at indices {idxs}")
            else:
                log(f"No duplicate longitudes in region {region}")

            ds = drop_duplicate_coords(ds, "longitude")
            ds = drop_duplicate_coords(ds, "latitude")
            
            lat_datasets.append(ds)
        log(f"Merging column {column} along latitude")
        # Merge regions in the same lon column along latitude
        merged_col = xr.concat(
            lat_datasets,
            dim="latitude",
            join="inner",
            coords="minimal",
            compat="override",
        )
        merged_col = merged_col.sortby("latitude").drop_duplicates(dim="latitude")
        #merged_col = merged_col.sortby("longitude").drop_duplicates(dim="longitude")
        merged_col = merged_col.sortby(["latitude", "longitude"]) #####
        log(f"Column {column} merged")

        lat_arrays.append(merged_col)

    log("Concatenating all columns along longitude")
    met = xr.concat(
        lat_arrays,
        dim="longitude",
        join="inner",
        coords="minimal",
        compat="override",
    )
    log(f"Full domain assembled — shape: {dict(met.sizes)}")

    met = met.drop_duplicates(dim="longitude")
    met = met.sortby(["latitude", "longitude", "time"])
    log("Filling potential 1-cell seams across region joins")
    met = fill_join_seams(met)
    met = met.assign_coords(
        latitude=met.latitude.astype(np.float32),
        longitude=met.longitude.astype(np.float32),
    )
    met = met.transpose(
        "model_level_number",
        "latitude",
        "longitude",
        "time",
        ...,
    )
    log("fixing attrs")
    print("Total X_wind Nans", int(met.x_wind[0,:,:,0].isnull().sum().values))
    for attr in ["units", "standard_name", "STASH"]:
      try:
        met.attrs.pop(attr)
      except:
        log(f"no attr {attr}")
        continue
    met.attrs["author"] = config.get("met_extract_author", "")
    met.attrs["created"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    met.attrs["transformations"] = "interpolated linearly in space to NAME resolution"#, interpolated linearly in time from 3-hourly to hourly"
    log(f"Final dataset: {met}")

    met_save_directory = resolve_config_value(config.get("met_save_directory", ""), config)
    filename = met_save_directory+domain+"_Met_"+str(year)+month+".nc"
    log(f"Writing to {filename}")

    t0 = datetime.datetime.now()
    met.to_netcdf(filename)
    elapsed = (datetime.datetime.now() - t0).total_seconds()
    file_stats = os.stat(filename)
    log(f"Saved in {elapsed:.1f}s — File size: {file_stats.st_size / (1024 * 1024):.1f} MB")

if args.delete_files:
    os.system("rm -r " + homefolder+domain+"_Met_"+str(year)+month+"_*")
            
