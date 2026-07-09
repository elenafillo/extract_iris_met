"""
Per-region extraction of UM meteorology data.

Extracts data from UM .pp files for a single world region, aligns winds onto the
mass grid, applies the target grid (footprint/regular = interpolate, native =
passthrough), slices to the region's bounds, and writes a per-region NetCDF
intermediate to scratch. The join step later stitches these intermediates into a
full domain.

Ported from ``satellite_met_1b1_fixed_v3.py`` (per-region body).
"""

import datetime
import os
import warnings

import numpy as np
import xarray as xr
import dask

from .config import Config, resolve_config_value
from .iris_io import get_Mk, load_iris, delete_iris
from .regions import get_saved_region_bounds
from .grid import build_target_grid


# Load only one in every three model levels (plus the surface) to reduce memory.
# range(1, 60)[2::3] → 3, 6, 9, ... 57; prefixed with level 1.
LEVELS = [1] + list(range(1, 60))[2::3]

# Constraints passed to iris.load to limit what is read from each region file.
# Note air_temperature resolves to two cubes (a 3D field with model levels and a
# 1.5 m surface field), so iris returns len(VARS)+1 cubes.
VARS = [
    "air_pressure",
    "air_pressure_at_sea_level",
    "air_temperature",
    "atmosphere_boundary_layer_thickness",
    "surface_air_pressure",
    "surface_upward_sensible_heat_flux",
    "upward_air_velocity",
    "x_wind",
    "y_wind",
]

# Variables kept and placed on the mass grid, selected by (name, want_levels).
# want_levels disambiguates names that resolve to both a 3D and a surface cube
# (air_temperature → keep the 3D field, drop the 1.5 m one). This intentionally
# omits surface_upward_sensible_heat_flux, matching the original script.
MASS_VARS = [
    ("air_pressure", True),
    ("air_pressure_at_sea_level", False),
    ("air_temperature", True),
    ("atmosphere_boundary_layer_thickness", False),
    ("surface_air_pressure", False),
    ("upward_air_velocity", True),
]

# Winds live on the staggered C-grid and are interpolated onto the mass grid.
WIND_VARS = [("x_wind", True), ("y_wind", True)]


def _has_levels(cube):
    """True if the cube carries a model_level_number coordinate."""
    return "model_level_number" in {co.name() for co in cube.coords()}


def _pick(cubes, name, want_levels):
    """
    Select the single cube matching ``name`` and level-dimension presence.

    Selecting by name + level presence (rather than positional index) makes the
    variable choice explicit and fails loudly if the archive's cube set changes,
    instead of silently returning the wrong array.

    Raises
    ------
    ValueError
        If zero or more than one cube matches.
    """
    matches = [c for c in cubes if c.name() == name and _has_levels(c) == want_levels]
    if len(matches) != 1:
        available = [(c.name(), _has_levels(c)) for c in cubes]
        raise ValueError(
            f"expected exactly 1 cube for name={name!r} want_levels={want_levels}, "
            f"found {len(matches)}. Loaded cubes (name, has_levels): {available}"
        )
    return matches[0]


def log(msg):
    """Print a timestamped message."""
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _dataarray_from_iris_safely(cube):
    """Convert an iris cube to an xarray DataArray, muting the timedelta-decode warning."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="In a future version, xarray will not decode timedelta values.*",
            category=FutureWarning,
        )
        return xr.DataArray.from_iris(cube)


def build_region_filepath(met_archive_directory, mk):
    """
    Build the (prefix, suffix) glob template that ``load_iris`` expands per region.

    The resulting filename is
    ``{prefix}{date}{suffix}{region}.pp[.gz]`` — matching every 3-hourly file for
    the month/region. Mk 6 files omit the ``_I_`` variable-set tag; Mk 7+ include
    it.

    Parameters
    ----------
    met_archive_directory : str
        Base met archive directory.
    mk : int
        UM Mk version.

    Returns
    -------
    list of str
        ``[prefix, suffix]`` for ``load_iris``.
    """
    base = os.path.join(met_archive_directory, f"UMG_Mk{mk}PT", "MO")
    if mk == 6:
        return [base, f"*.UMG_Mk{mk}_L59PT"]
    return [base, f"*.UMG_Mk{mk}_I_L59PT"]


def extract_region(
    domain_key,
    year,
    month,
    region_id,
    cfg,
    target=None,
    use_interp=None,
    scratch_dir=None,
    save=True,
    cleanup_pp=True,
    day=None,
):
    """
    Extract a single world region for a domain/year/month (or single day).

    Loads the region's monthly UM data, aligns winds onto the mass grid, applies
    the target grid, slices to the region's bounds (handling dateline crossings),
    and writes a per-region NetCDF intermediate to scratch.

    Parameters
    ----------
    domain_key : str
        Domain key (e.g., 'SA').
    year : int
        Year (e.g., 2016).
    month : int or str
        Calendar month, as an int 1-12 or a "01".."12" string.
    region_id : int
        World region ID (1-14).
    cfg : met_extract.config.Config
        Configuration object.
    target : tuple of np.ndarray, optional
        Precomputed ``(target_lat, target_lon)`` 1D arrays. If None, the target
        grid is built from the domain's grid spec via ``build_target_grid``.
    use_interp : bool, optional
        Whether to interpolate onto the target grid. If None, inferred from the
        grid mode (footprint/regular → True, native → False).
    scratch_dir : str, optional
        Directory for per-region intermediates. Defaults to
        ``{scratch_path}/files/``.
    save : bool, optional
        If True (default), write the intermediate and return its path. If False,
        return the in-memory dataset instead.
    cleanup_pp : bool, optional
        If True (default), delete the region's unzipped .pp scratch files after
        a successful load to conserve scratch space.
    day : int or str, optional
        If given, restrict the load to a single day (that day's ~8 3-hourly
        files) instead of the whole month — a cheap pipeline smoke test. The
        intermediate filename then carries the full YYYYMMDD tag.

    Returns
    -------
    pathlib.Path or xarray.Dataset
        Path to the per-region NetCDF (if ``save``), else the dataset.
    """
    if isinstance(cfg, dict):
        cfg = Config(cfg)

    # Normalise month to a "01".."12" string; build the glob key (and file tag).
    # date drives both the archive glob (MO{date}*) and the intermediate name.
    month_int = int(month)
    month_str = f"{month_int:02d}"
    if day is not None:
        date = f"{year}{month_str}{int(day):02d}"
    else:
        date = f"{year}{month_str}"

    domain_cfg = cfg.get_domain(domain_key)
    domain_name = domain_cfg["domain_name"]

    scratch_root = resolve_config_value(cfg.get("scratch_path", ""), cfg.data)
    if scratch_dir is None:
        scratch_dir = os.path.join(scratch_root, "files")
    os.makedirs(scratch_dir, exist_ok=True)

    met_archive_directory = resolve_config_value(cfg.get("met_archive_directory", ""), cfg.data)

    mk = get_Mk(year, month_int)
    filepath = build_region_filepath(met_archive_directory, mk)

    # Resolve the target grid if not provided.
    if target is None:
        target_lat, target_lon, inferred_interp = build_target_grid(domain_cfg, cfg, mk=mk)
        if use_interp is None:
            use_interp = inferred_interp
    else:
        target_lat, target_lon = target
        if use_interp is None:
            grid_mode = domain_cfg.get("grid", {}).get("mode", "footprint")
            use_interp = grid_mode != "native"

    region_bounds = get_saved_region_bounds()

    region_start = datetime.datetime.now()
    log(f"************ Region {region_id} ({domain_name} {date}, Mk{mk}) ************")

    with dask.config.set(**{"array.slicing.split_large_chunks": True}), \
            xr.set_options(keep_attrs=True):
        t0 = datetime.datetime.now()
        cube = load_iris(filepath, mk, date, VARS, region_id, scratch_root + os.sep)
        log(f"region {region_id} loaded in {(datetime.datetime.now() - t0).total_seconds():.1f}s")

        # Combine mass-grid variables; align the staggered winds onto that grid.
        # Cubes are selected by name + level presence (see _pick) so a changed
        # archive raises rather than silently mis-selecting.
        most_variables = xr.combine_by_coords(
            [_dataarray_from_iris_safely(_pick(cube, name, lvl)) for name, lvl in MASS_VARS]
        ).sel(model_level_number=LEVELS)
        x_wind = xr.combine_by_coords(
            [_dataarray_from_iris_safely(_pick(cube, "x_wind", True))], compat="override"
        ).sel(model_level_number=LEVELS)
        y_wind = xr.combine_by_coords(
            [_dataarray_from_iris_safely(_pick(cube, "y_wind", True))], compat="override"
        ).sel(model_level_number=LEVELS)
        del cube

        all_variables = xr.combine_by_coords(
            [
                most_variables,
                x_wind.interp(
                    latitude=most_variables.latitude.values,
                    longitude=most_variables.longitude.values,
                ),
                y_wind.interp(
                    latitude=most_variables.latitude.values,
                    longitude=most_variables.longitude.values,
                ),
            ],
            compat="override",
        )
        del most_variables, x_wind, y_wind

        t0 = datetime.datetime.now()
        all_variables.load()
        log(f"loaded combined dataset into memory in {(datetime.datetime.now() - t0).total_seconds():.1f}s")

        if cleanup_pp:
            # Safe to delete the unzipped .pp files now the data is in memory.
            # (iris.load is lazy, so this must come after .load(), not before.)
            try:
                delete_iris(scratch_root + os.sep, date, region_id)
            except Exception as exc:  # non-fatal; just leaves scratch files behind
                log(f"could not clean up .pp scratch for region {region_id}: {exc}")

        # Normalise longitude to [-180, 180].
        all_variables = all_variables.assign_coords(
            longitude=(((all_variables.longitude + 180) % 360) - 180)
        )

        # Ensure a real 'time' dimension exists.
        if "time" not in list(all_variables.sizes.keys()):
            all_variables = all_variables.stack(newtime=["forecast_period", "forecast_reference_time"])
            all_variables = all_variables.swap_dims({"newtime": "time"})
            # Retain forecast_period / forecast_reference_time as time-indexed
            # coords (provenance: which model run each step is from + its lead
            # time). Capture their values against the new time axis, drop the
            # helper stacking index, then re-attach as clean 1-D coords.
            fp_vals = all_variables["forecast_period"].values
            frt_vals = all_variables["forecast_reference_time"].values
            all_variables = all_variables.drop_vars(
                ["forecast_period", "forecast_reference_time", "newtime"]
            )
            all_variables = all_variables.assign_coords(
                forecast_period=("time", fp_vals),
                forecast_reference_time=("time", frt_vals),
            )
            log("time dimension constructed; forecast_period + forecast_reference_time retained")
        else:
            all_variables = all_variables.transpose(
                "model_level_number", "latitude", "longitude", "time", ...
            )

        # Guard: the time axis is valid_time = forecast_reference_time +
        # forecast_period. Retaining those two as provenance coords only makes
        # sense if each valid time comes from a *unique* (reference_time, period)
        # pair. If different pairs collide onto the same valid time we'd get
        # duplicate timesteps with ambiguous provenance — fail loudly here rather
        # than silently corrupting the series.
        time_vals = all_variables["time"].values
        n_dup = int(len(time_vals) - len(np.unique(time_vals)))
        if n_dup:
            raise ValueError(
                f"Region {region_id} {date}: {n_dup} duplicate timestamp(s) out of "
                f"{len(time_vals)} after building the time axis. Some valid times "
                f"arise from more than one (forecast_reference_time, forecast_period) "
                f"combination, so those coordinates cannot be retained unambiguously. "
                f"Inspect the source files for overlapping forecast periods / "
                f"reference times."
            )

        # Apply the target grid.
        if use_interp:
            all_variables = all_variables.interp(latitude=target_lat, longitude=target_lon)
            log(f"interpolated region {region_id} onto target grid ({len(target_lat)}x{len(target_lon)})")
        else:
            log(f"native mode: keeping region {region_id} on its UM grid (no interpolation)")

        # Slice to this region's bounds, handling dateline crossing.
        lat_min, lat_max = region_bounds[region_id][0], region_bounds[region_id][1]
        lon_min, lon_max = region_bounds[region_id][2], region_bounds[region_id][3]
        all_variables = all_variables.sel(latitude=slice(lat_min, lat_max))
        if lon_min <= lon_max:
            all_variables = all_variables.sel(longitude=slice(lon_min, lon_max))
        else:
            east = all_variables.sel(longitude=slice(lon_min, 180.0))
            west = all_variables.sel(longitude=slice(-180.0, lon_max))
            all_variables = xr.concat([east, west], dim="longitude")
            log(f"region {region_id} crosses dateline; concatenated east/west slices")

        result = all_variables.sortby("time")
        log(f"region {region_id} extracted; dims {dict(result.sizes)}")

        if not save:
            log(
                f"---- Region {region_id} done in "
                f"{(datetime.datetime.now() - region_start).total_seconds():.1f}s (not saved) ----"
            )
            return result

        result = result.chunk(
            {"model_level_number": -1, "time": 50, "latitude": 50, "longitude": 50}
        )

        filename = os.path.join(scratch_dir, f"{domain_name}_Met_{date}_{region_id}.nc")
        t0 = datetime.datetime.now()
        result.to_netcdf(filename)
        size_mb = os.stat(filename).st_size / (1024 * 1024)
        log(
            f"---- Region {region_id} saved to {filename} in "
            f"{(datetime.datetime.now() - t0).total_seconds():.1f}s ({size_mb:.1f} MB); "
            f"total {(datetime.datetime.now() - region_start).total_seconds():.1f}s ----"
        )

    return filename
