# extract_iris_met
This repo has functions to extract meteorological datasets from the UM model hosted on jasmin (iris .pp files)

## File Structure
The UM model is hosted on JASMIN at `/gws/nopw/j04/name/met_archive/Global/`. From 2011, it gets released in "Mk" blocks, under folders labelled `UMG_Mk{Mk_number}PT`. The following Mk numbers correspond to the following timeperiods: 
| Mk Number    | Dates contained |
| -------- | ------- |
| Mk6  | Jan 2011 - March 2013    |
| Mk7  | Apr 2013 - Jun 2014    |
| Mk8  | Jul 2014 - Jul 2015    |
| Mk9  | Aug 2015 - Jun 2017    |
| Mk10  | Jul 2017 - May 2022    |
| Mk11  | Jun 2022 -     |

In each folder, the 3-hourly files have format `MO{year}{month}{day}{day_period}.UMG_Mk{Mk_number}_{variable_set}_L59PT{world_region}.pp`, eg `MO201403080000.UMG_Mk7_I_L59PT10.pp`
- Date in format YYYYMMDD
- Day period, out of {0000, 0300, 0600, 0900, 1200, etc until 2100}
- variable_set out of {I, M}. Each file contains a different list of variables (see lists at the bottom)
- world_region, from 1-14. see map below
- The files for Mk6-Mk9 are zipped (format .pp.gz) and need to be unzipped before being read. Mk10 onwards can be read directly.

![image](https://github.com/user-attachments/assets/f6dc8296-f87f-4d82-bcf5-533147d8e9a3)



## Step-by-step of extracting met
1. Define domain you want to extract, and which regions it covers [To Do - Add visualisations on notebook]
2. Use "extract_one_by_one" file to unzip any necessary files, copy them to scratch, extract the necessary data from each region and save it, separately
3. Use "join_regions" to join the extracted regions, to cover your specified domain. Currently the setup can join two regions (along the latitude axis, ie side by side, or along the longitude axis, ie on top of each other), or four regions (forming a rectangle).


## Variable lists:
| variable_set    | Variable Number | Name | Notes |
| -------- | ------- |------- |------- |
| M  | 0    |  m01s05i220 |   |
| M  | 1    |  m01s05i223 |   |
| M  | 2    |  air_pressure_at_convective_cloud_base / (Pa) |   |
| M  | 3    | convective_rainfall_flux / (kg m-2 s-1) |   |
| M  | 4    | convective_snowfall_flux / (kg m-2 s-1) |   |
| M  | 5    | high_type_cloud_area_fraction  |   |
| M  | 6    |low_type_cloud_area_fraction  |   |
| M  | 7    | mass_fraction_of_cloud_ice_in_air |  has height variable   |
| M  | 8    |  mass_fraction_of_cloud_liquid_water_in_air | has height variable  |
| M  | 9    | medium_type_cloud_area_fraction |   |
| M  | 10    |stratiform_rainfall_flux  |   |
| M  | 11    | stratiform_snowfall_flux |   |
| I  | 0    |  m01s03i026 |   |
| I  | 1    |  m01s03i319  | has "pseudo level" variable  |
| I  | 2    |  anopy water on tiles / (kg/m^2)|   |
| I  | 3    | m01s03i462 |   |
| I  | 4    | atmosphere_downward_eastward_stress / (Pa) |   |
| I  | 5    | atmosphere_downward_northward_stress / (Pa)   |   |
| I  | 6    |air_pressure / (Pa)  | has height variable  |
| I  | 7    | air_pressure_at_sea_level / (Pa) |    |
| I  | 8    |  air_temperature / (K)  |   |
| I  | 9    |air_temperature / (K) | has height variable  |
| I  | 10    |atmosphere_boundary_layer_thickness |   |
| I  | 11    | moisture_content_of_soil_layer  | has depth variable  |
| I  | 12    | specific_humidity / (kg kg-1)  |  has height variable |
| I  | 13    | surface_air_pressure / (Pa)   |   |
| I  | 14    | surface_upward_sensible_heat_flux / (W m-2) |   |
| I  | 15    |upward_air_velocity / (m s-1)   | has height variable  |
| I  | 16    | x_wind / (m s-1)  | has height variable  |
| I  | 17    | y_wind / (m s-1)  | has height variable  |

