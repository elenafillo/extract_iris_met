
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


def get_default_paths():
    stratch = "/work/scratch-nopw2/elenafi/"

    paths = {"stratch":stratch}

    return paths



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

def daterange(start_date, end_date, a_day_only=False):
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