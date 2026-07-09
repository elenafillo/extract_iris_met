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
from .iris_io import load_files, delete_iris
from .grid import build_target_grid
from .sources import get_source


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
    "atmosphere_downward_eastward_stress",
    "atmosphere_downward_northward_stress",
    "specific_humidity",
    "surface_air_pressure",
    "surface_upward_sensible_heat_flux",
    "upward_air_velocity",
    "x_wind",
    "y_wind",
]

# --- Instantaneous fields (snapshot at the valid time) ---
# Already on the mass grid, selected by (name, want_levels). want_levels
# disambiguates names that resolve to both a 3D and a surface cube
# (air_temperature → keep the 3D field, drop the 1.5 m one).
MASS_VARS = [
    ("air_pressure", True),
    ("air_pressure_at_sea_level", False),
    ("air_temperature", True),
    ("atmosphere_boundary_layer_thickness", False),
    ("specific_humidity", True),
    ("surface_air_pressure", False),
    ("upward_air_velocity", True),
]

# Staggered 3D winds on the C-grid: subsample levels, then interpolate onto the
# mass grid.
WIND_VARS = [("x_wind", True), ("y_wind", True)]

# --- Time-averaged fields (3-hour means over the forecast interval) ---
# In the archive these are stamped at the interval MIDPOINT, carry time bounds,
# and come from a *different* forecast run than the instantaneous fields — so they
# live on a different (time, forecast_period, forecast_reference_time) grid and
# cannot be merged directly. We re-stamp them to the interval END (which equals
# the instantaneous valid time), drop their own forecast bookkeeping, and merge on
# valid time. Their averaging convention is recorded in each variable's attrs.
AVERAGED_MASS_VARS = ["surface_upward_sensible_heat_flux"]          # on the mass grid
AVERAGED_STAGGERED_VARS = [                                         # staggered → interp
    "atmosphere_downward_eastward_stress",   # offset in longitude (like x_wind)
    "atmosphere_downward_northward_stress",  # offset in latitude  (like y_wind)
]
_AVERAGED_NOTE = (
    "3-hour time-mean ending at the timestamp; re-stamped from the UM interval "
    "midpoint to align with the instantaneous fields."
)


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


def _restamp_to_interval_end(cube):
    """
    Re-stamp a time-averaged cube's time onto its interval END, dropping bounds.

    Averaged fields (fluxes/stresses) are stored at the interval midpoint with
    time bounds [start, end]; the end equals the instantaneous fields' valid time.
    Moving the point to the end lets them share one time axis. Modifies and
    returns the cube.
    """
    tc = cube.coord("time")
    if tc.has_bounds():
        tc.points = tc.bounds.max(axis=-1)   # interval end (robust to bound order)
        tc.bounds = None
    if cube.coords("forecast_period") and cube.coord("forecast_period").has_bounds():
        cube.coord("forecast_period").bounds = None
    return cube


def _build_time_axis(ds, keep_provenance):
    """
    Put a dataset/dataarray onto a real ``time`` dimension using its ``time`` aux
    coordinate, collapsing whatever forecast dims that coord spans.

    The instantaneous fields arrive on a (forecast_period, forecast_reference_time)
    grid with ``time`` as an aux coord; the averaged fields may span only one of
    those. Stacking exactly the dims ``time`` depends on handles both.

    Parameters
    ----------
    ds : xarray.Dataset or DataArray
    keep_provenance : bool
        If True, retain forecast_period/forecast_reference_time as time-indexed
        coords (used for the instantaneous met); if False, drop them (the averaged
        fields' forecast bookkeeping differs and is not meaningful once merged).
    """
    if "time" in ds.dims:
        return ds

    time_dims = list(ds["time"].dims)
    if not time_dims:                     # scalar time → length-1 time dim
        return ds.expand_dims("time")

    ds = ds.stack(newtime=time_dims).swap_dims({"newtime": "time"})
    if keep_provenance:
        fp = ds["forecast_period"].values
        frt = ds["forecast_reference_time"].values
        ds = ds.drop_vars(["forecast_period", "forecast_reference_time", "newtime"])
        ds = ds.assign_coords(
            forecast_period=("time", fp),
            forecast_reference_time=("time", frt),
        )
    else:
        ds = ds.drop_vars(
            ["forecast_period", "forecast_reference_time", "newtime"], errors="ignore"
        )
    return ds.sortby("time")


def _check_unique_time(ds, region_id, date):
    """Fail loudly if the built time axis has duplicate timestamps."""
    tv = ds["time"].values
    n_dup = int(len(tv) - len(np.unique(tv)))
    if n_dup:
        raise ValueError(
            f"Region {region_id} {date}: {n_dup} duplicate timestamp(s) out of "
            f"{len(tv)} after building the time axis. Some valid times arise from "
            f"more than one (forecast_reference_time, forecast_period) combination, "
            f"so those coordinates cannot be retained unambiguously. Inspect the "
            f"source files for overlapping forecast periods / reference times."
        )


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
    source=None,
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

    # Resolve the met data type ("source") — where the files live, the Mk
    # calendar, level set, and region scheme all come from it.
    if source is None:
        source = get_source(domain_cfg.get("data_type", "UM_Global"), cfg)

    scratch_root = resolve_config_value(cfg.get("scratch_path", ""), cfg.data)
    if scratch_dir is None:
        scratch_dir = os.path.join(scratch_root, "files")
    os.makedirs(scratch_dir, exist_ok=True)

    mk = source.get_mk(year, month_int)
    levels = source.levels()

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

    region_bounds = source.region_bounds()
    if region_bounds is None:
        raise ValueError(
            f"source {source.name!r} is not tiled (region_scheme='none'); "
            f"per-region extract needs a region scheme."
        )

    mk_label = f"Mk{mk}" if mk is not None else source.name
    region_start = datetime.datetime.now()
    log(f"************ Region {region_id} ({domain_name} {date}, {mk_label}) ************")

    with dask.config.set(**{"array.slicing.split_large_chunks": True}), \
            xr.set_options(keep_attrs=True):
        t0 = datetime.datetime.now()
        files = source.list_files(date, region=region_id, mk=mk)
        cube = load_files(files, VARS, scratch_root + os.sep)
        log(f"region {region_id} loaded in {(datetime.datetime.now() - t0).total_seconds():.1f}s")

        # --- Instantaneous group: mass-grid variables + staggered winds -------
        # Cubes are selected by name + level presence (see _pick) so a changed
        # archive raises rather than silently mis-selecting.
        most_variables = xr.combine_by_coords(
            [_dataarray_from_iris_safely(_pick(cube, name, lvl)) for name, lvl in MASS_VARS]
        ).sel(model_level_number=levels)
        mass_lat = most_variables.latitude.values
        mass_lon = most_variables.longitude.values

        winds = []
        for name, _ in WIND_VARS:
            da = xr.combine_by_coords(
                [_dataarray_from_iris_safely(_pick(cube, name, True))], compat="override"
            ).sel(model_level_number=levels)
            winds.append(da.interp(latitude=mass_lat, longitude=mass_lon))

        inst = xr.combine_by_coords([most_variables, *winds], compat="override")
        del most_variables, winds

        # --- Averaged group: fluxes/stresses, re-stamped to the interval end ---
        # These are 3-hour means from a different forecast run, so they can't be
        # merged with the instantaneous fields until re-stamped onto the shared
        # valid-time axis (see AVERAGED_* and _restamp_to_interval_end).
        avg_arrays = []
        for name in AVERAGED_MASS_VARS:
            c = _restamp_to_interval_end(_pick(cube, name, False))
            avg_arrays.append(_dataarray_from_iris_safely(c))
        for name in AVERAGED_STAGGERED_VARS:
            c = _restamp_to_interval_end(_pick(cube, name, True))
            da = _dataarray_from_iris_safely(c)
            if "model_level_number" in da.coords:
                da = da.drop_vars("model_level_number")
            avg_arrays.append(da.interp(latitude=mass_lat, longitude=mass_lon))
        del cube

        # --- Load into memory, then free the .pp scratch --------------------
        t0 = datetime.datetime.now()
        inst.load()
        avg_arrays = [da.load() for da in avg_arrays]
        log(f"loaded combined dataset into memory in {(datetime.datetime.now() - t0).total_seconds():.1f}s")

        if cleanup_pp:
            # Safe to delete the unzipped .pp files now the data is in memory.
            # (iris.load is lazy, so this must come after .load(), not before.)
            try:
                delete_iris(scratch_root + os.sep, date, region_id)
            except Exception as exc:  # non-fatal; just leaves scratch files behind
                log(f"could not clean up .pp scratch for region {region_id}: {exc}")

        # --- Build the valid-time axis per group, then merge on time ---------
        # Instantaneous fields retain forecast_period/forecast_reference_time as
        # provenance; each averaged field builds its own time axis (its forecast
        # dims may differ) and drops that bookkeeping before merging on valid time.
        inst = _build_time_axis(inst, keep_provenance=True)
        _check_unique_time(inst, region_id, date)
        log("time dimension constructed; forecast_period + forecast_reference_time retained")

        if avg_arrays:
            avg_das = []
            for da in avg_arrays:
                da = _build_time_axis(da, keep_provenance=False)
                _check_unique_time(da, region_id, date)
                da.attrs["averaging"] = _AVERAGED_NOTE
                avg_das.append(da)
            all_variables = xr.merge([inst, *avg_das], join="inner", compat="override")
            log(f"merged {len(avg_das)} time-averaged field(s) onto the valid-time grid")
        else:
            all_variables = inst
        del inst

        # Normalise longitude to [-180, 180].
        all_variables = all_variables.assign_coords(
            longitude=(((all_variables.longitude + 180) % 360) - 180)
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
