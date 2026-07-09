"""
CF-convention metadata for met_extract output.

Single place that owns the coordinate and global attributes written into the
yearly zarr stores, so the output is self-describing (analysis-ready) and its
provenance is auditable. Organisation-specific fields (institution, references,
title, source, comment) are read from the ``metadata:`` block in config.yaml;
empty values are omitted rather than written as blank attributes.

Variable-level attributes (units, standard_name, …) are intentionally left as
carried through from iris — the extraction/join steps run with
``xr.set_options(keep_attrs=True)`` so those survive.
"""

import datetime


CONVENTIONS_DEFAULT = "CF-1.10"

# CF attributes for coordinate variables, applied where the coordinate exists.
CF_COORD_ATTRS = {
    "lat": {
        "standard_name": "latitude",
        "long_name": "latitude",
        "units": "degrees_north",
        "axis": "Y",
    },
    "lon": {
        "standard_name": "longitude",
        "long_name": "longitude",
        "units": "degrees_east",
        "axis": "X",
    },
    "time": {
        "standard_name": "time",
        "long_name": "time",
        "axis": "T",
    },
    "levels": {
        "long_name": "UM model level number (subsampled: level 1 plus every 3rd level)",
        "positive": "up",
    },
    "forecast_period": {
        "standard_name": "forecast_period",
        "long_name": "time since the forecast reference time",
    },
    "forecast_reference_time": {
        "standard_name": "forecast_reference_time",
        "long_name": "model run (analysis) reference time",
    },
}

# For these coords, keep any iris-provided attrs (esp. units) and only fill gaps;
# for the rest, the CF attrs above are authoritative.
_FILL_ONLY = {"forecast_period", "forecast_reference_time"}


def _clean(mapping):
    """Drop keys whose value is None or an empty/whitespace-only string."""
    out = {}
    for k, v in mapping.items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        out[k] = v
    return out


def apply_cf_metadata(ds, cfg, *, mk, grid_mode, domain_name, year, use_interp=True):
    """
    Stamp CF coordinate and global attributes onto a monthly/period dataset.

    Coordinate attributes are set for whichever of lat/lon/time/levels/
    forecast_* are present. Global attributes combine config-provided fields (the
    ``metadata:`` block) with auto-generated defaults; empty config fields are
    omitted. Existing variable attributes carried through from iris are untouched.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to annotate (coords already renamed to lat/lon/levels).
    cfg : met_extract.config.Config
        Configuration object (provides the metadata: block and author).
    mk : int
        UM Mk version the data came from.
    grid_mode : str
        Grid mode used ('footprint' | 'regular' | 'native').
    domain_name : str
        Domain name (e.g., 'INDIA').
    year : int
        Year (used for the auto title).
    use_interp : bool
        Whether fields were interpolated onto a target grid (affects the comment).

    Returns
    -------
    xarray.Dataset
        The same dataset, with attributes set (modified in place and returned).
    """
    # --- coordinate attributes ---
    for name, attrs in CF_COORD_ATTRS.items():
        if name not in ds.variables:
            continue
        existing = dict(ds[name].attrs)
        if name in _FILL_ONLY:
            ds[name].attrs = {**attrs, **existing}   # iris attrs win (keep real units)
        else:
            ds[name].attrs = {**existing, **attrs}   # CF attrs win

    # --- global attributes ---
    md = cfg.get("metadata", {}) or {}
    if not isinstance(md, dict):
        md = {}

    conventions = md.get("conventions") or CONVENTIONS_DEFAULT
    title = md.get("title") or f"UM meteorology, {domain_name} {year}"
    source = md.get("source") or (
        f"Met Office Unified Model (UM) Mk{mk}, extracted from the NAME global met archive"
    )

    auto_comment = (
        "x_wind/y_wind interpolated from the UM staggered (Arakawa C) grid onto the "
        "mass/pressure grid; 1-in-3 model levels retained (level 1 plus every 3rd); "
        f"grid_mode={grid_mode}"
    )
    if not use_interp:
        auto_comment += "; fields kept on the native UM grid (no spatial interpolation)"
    user_comment = (md.get("comment") or "").strip()
    comment = f"{auto_comment}. {user_comment}" if user_comment else auto_comment

    now = datetime.datetime.now().isoformat(timespec="seconds")
    history = f"{now}: extracted and joined by met_extract"

    ds.attrs.update(_clean({
        "Conventions": conventions,
        "title": title,
        "institution": md.get("institution", ""),
        "source": source,
        "references": md.get("references", ""),
        "history": history,
        "comment": comment,
        "author": cfg.get("met_extract_author", ""),
        "grid_mode": grid_mode,
        "mk_version": int(mk),
    }))

    return ds
