
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


def load_config():
    """
    Load the project configuration file and normalize required keys.

    The function first looks for `config.yaml` in the current working directory,
    and falls back to `config.yml` if needed. The parsed YAML must produce a
    dictionary-like object; otherwise a ValueError is raised.

    For portability across user accounts, this helper guarantees that a `user`
    value exists in the returned config. If `user` is missing or empty in the
    file, it is populated from the `USER` environment variable (or an empty
    string if that variable is unavailable).

    Returns
    -------
    dict
        Parsed and normalized configuration mapping.
    """
    config_path = "config.yaml" if os.path.exists("config.yaml") else "config.yml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ValueError("config.yaml did not parse to a dictionary")

    if not config.get("user"):
        config["user"] = os.environ.get("USER", "")

    return config


def resolve_config_value(value, config):
    """
    Resolve string templates in individual config values.

    If `value` is a string, this function applies Python `str.format(**config)`
    so placeholders such as `{user}` are expanded from keys in the config
    mapping. Non-string values are returned unchanged.

    Missing placeholders are handled safely: if a required key is not present,
    the original string is returned without raising an exception. This lets the
    caller decide whether unresolved placeholders are acceptable.

    Parameters
    ----------
    value : Any
        Raw config value to resolve.
    config : dict
        Configuration mapping used as format context.

    Returns
    -------
    Any
        Resolved value (for strings) or the original value (for non-strings or
        unresolved templates).
    """
    if not isinstance(value, str):
        return value
    try:
        return value.format(**config)
    except KeyError:
        return value

def get_saved_region_bounds():
    """
    Return the saved latitude/longitude bounds for each world region.

    The returned mapping is used by the extraction and join scripts to work
    out which global region files overlap a given domain. The bounds reflect
    the saved region layout used elsewhere in the repository, including the
    dateline-crossing regions.

    Returns
    -------
    dict
        Mapping of region ID to [min_lat, max_lat, min_lon, max_lon].
    """
    """
    # these are actually incorrect but plot the correct lines - something to do with wrapping around the edges
    
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
    """
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

    return region_bounds

def find_overlapping_regions(min_lat, max_lat, min_lon, max_lon):
    """
    Find the world-region IDs that overlap a latitude/longitude box.

    This is a convenience wrapper around the saved region bounds. It checks
    each world region and returns the IDs whose latitude and longitude ranges
    intersect the supplied bounding box.

    Parameters
    ----------
    min_lat, max_lat : float
        Latitude limits of the box to test.
    min_lon, max_lon : float
        Longitude limits of the box to test.

    Returns
    -------
    list of int
        Region IDs that intersect the supplied bounding box.
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
        config = load_config()
        scripts_text = resolve_config_value(config.get("scripts_text_save_location", ""), config)
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
    """
    Return the configured edge size for a domain, or the default fallback.

    The config file can define per-domain values for `edge_size_lat` and
    `edge_size_lon`. If a domain does not define the requested size type, the
    global `default_edge_size` value is returned instead.

    Parameters
    ----------
    domain : str
        Domain key from config.yaml, for example ``SA`` or ``INDIA``.
    size_type : str
        Which size setting to fetch, typically ``edge_size_lat`` or
        ``edge_size_lon``.

    Returns
    -------
    list
        Two-element list describing the requested edge size.
    """

    # Retrieve the domain or global default edge size from the yaml configuration file
    config = load_config()

    default_edge_size = config.get('default_edge_size', [100, 100])

    try:
        return config['domains'][domain].get(size_type, default_edge_size)
    except KeyError:
        return default_edge_size

def build_domain_grid(global_grid, regions_to_include):
    """
    Trim a global region grid down to just the regions needed for one domain.

    Cells that are not part of the requested domain are replaced with None.
    Entire rows and columns that become empty after filtering are removed so the
    result is the smallest rectangular grid that still preserves the target
    layout.

    Parameters
    ----------
    global_grid : list of list of int
        Full region grid used by the domain-joining logic.
    regions_to_include : list of int
        Region IDs that should remain in the returned grid.

    Returns
    -------
    list of list
        Trimmed grid containing only the requested regions.

    Examples
    --------
    >>> build_domain_grid([[2, 3], [6, 7]], [3, 7])
    [[None, 3], [None, 7]]
    """
    # Create a mask for the regions to include
    trimmed_grid = []
    for row in global_grid:
        new_row = [cell if cell in regions_to_include else None for cell in row]
        if any(cell is not None for cell in new_row):  # Keep rows with at least one valid region
            trimmed_grid.append(new_row)

    # Now trim columns (transpose → filter → transpose back)
    transposed = list(map(list, zip(*trimmed_grid)))
    trimmed_transposed = [
        col for col in transposed if any(cell is not None for cell in col)
    ]
    final_grid = list(map(list, zip(*trimmed_transposed)))  # Back to row-major

    return final_grid

def drop_duplicate_coords(ds, dim):
    """
    Drop duplicate coordinate values along a given dimension.

    This helper keeps the first occurrence of each coordinate value and drops
    later duplicates. It is used to clean up stitched datasets where floating-
    point precision or concatenation can create repeated latitude or longitude
    coordinate entries.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to clean.
    dim : str
        Coordinate dimension to deduplicate.

    Returns
    -------
    xarray.Dataset
        Dataset with duplicate coordinate values removed along `dim`.
    """
    coord_vals = ds[dim].values
    _, unique_idx = np.unique(coord_vals, return_index=True)
    if len(unique_idx) < len(coord_vals):
        print(f"Dropping {len(coord_vals) - len(unique_idx)} duplicate values in '{dim}'")
    return ds.isel({dim: sorted(unique_idx)})