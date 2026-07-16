"""
Static ancillary topography field extraction.

Unlike the met pipeline, ancillary fields are a single static array per domain: 
one native-grid file in, one NetCDF out. 
Data is processed to line up pixel-for-pixel with the met/footprint data it will be stacked alongside.

"""

import datetime
import os
from pathlib import Path

import iris
import xarray as xr

from .config import Config, resolve_config_value
from .grid import build_target_grid
from .rotated import latlon_target_cube, regrid_to_latlon, rotated_pole_attrs


def _load_single_cube(filepath, standard_name):
    """Load `filepath` and return its one cube matching `standard_name`."""
    cubes = iris.load(filepath)
    matches = [c for c in cubes if c.name() == standard_name]
    if len(matches) != 1:
        available = [c.name() for c in cubes]
        raise ValueError(
            f"expected exactly 1 '{standard_name}' cube in {filepath}, found "
            f"{len(matches)}. Cubes in file: {available}"
        )
    return matches[0]


def _ancillary_output_path(cfg, domain_name, field):
    save_root = resolve_config_value(cfg.get("ancillary_save_directory", "data/"), cfg.data)
    return Path(save_root) / domain_name / f"{domain_name}_{field}.nc"


def _ancillary_global_attrs(cfg, domain_name, source_file):
    """Global attrs in the same spirit as metadata.apply_cf_metadata, sized for a static field (no mk/year/grid_mode)."""
    md = cfg.get("metadata", {}) or {}
    now = datetime.datetime.now().isoformat(timespec="seconds")
    attrs = {
        "Conventions": md.get("conventions") or "CF-1.10",
        "title": md.get("title") or f"Static topography, {domain_name}",
        "institution": md.get("institution", ""),
        "references": md.get("references", ""),
        "source": os.path.basename(str(source_file)),
        "history": f"{now}: extracted by met_extract.topog",
        "author": cfg.get("met_extract_author", ""),
    }
    return {k: v for k, v in attrs.items() if v}


def extract_topog(domain_key, cfg):
    """
    Regrid a domain's native-grid topography onto its met target grid and save it.

    Parameters
    ----------
    domain_key : str
        Resolved domain key (e.g. 'NZ'), as returned by Config.resolve_domain_name.
        Callers going through the CLI resolve this once at the argument-parsing
        boundary, matching extract_region/extract_single.
    cfg : met_extract.config.Config

    Returns
    -------
    pathlib.Path
        Path to the written NetCDF.
    """
    if isinstance(cfg, dict):
        cfg = Config(cfg)

    domain_cfg = cfg.get_domain(domain_key)
    domain_name = domain_cfg["domain_name"]
    source_name = domain_cfg.get("data_type", "UM_Global")

    overrides = (cfg.get("ancillary_types", {}) or {}).get(source_name, {}) or {}
    topog_file = resolve_config_value(overrides.get("native_topog_file"), cfg.data)
    if not topog_file:
        raise ValueError(
            f"No ancillary_types.{source_name}.native_topog_file configured "
            f"(needed for domain {domain_name})"
        )

    cube = _load_single_cube(topog_file, "surface_altitude")
    pole_attrs = rotated_pole_attrs(cube)

    target_lat, target_lon, _ = build_target_grid(domain_cfg, cfg)
    target_cube = latlon_target_cube(target_lat, target_lon)
    regridded = regrid_to_latlon(cube, target_cube)

    da = xr.DataArray(
        regridded.data.astype("float32"),
        dims=("lat", "lon"),
        coords={
            "lat": target_lat.astype("float32"),
            "lon": target_lon.astype("float32"),
        },
        name="surface_altitude",
        attrs={"units": "m", "standard_name": "surface_altitude", **pole_attrs},
    )
    ds = da.to_dataset()
    ds.attrs.update(_ancillary_global_attrs(cfg, domain_name, topog_file))

    out_path = _ancillary_output_path(cfg, domain_name, "topog")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(out_path)
    return out_path
