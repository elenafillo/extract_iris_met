"""
Met data-type ("source") abstraction.

Each met product — the global UM (`UM_Global`), the 1.5 km UK LimitedArea
(`UM1p5km`), the New Zealand convective-scale model (`NZCSM`), … — varies on
almost every axis: archive path, filename pattern, Mk calendar, region scheme,
grid projection, level count, cadence, compression, and whether it is tiled into
regions (needing a join) or delivered as a single file over the whole domain.

A :class:`MetSource` is a declarative descriptor that captures those axes so the
extraction/join/run pipeline can dispatch on it instead of hardcoding
`UM_Global`. Built-in sources live in :data:`SOURCES`; :func:`get_source`
resolves one (applying config overrides such as archive paths).

This module is the *data model* only — it does not read cubes or regrid. The
grid-type handlers (regular vs rotated-pole) and the region-scheme bounds live in
`grid.py` / `regions.py`; this module names which ones a source uses.
"""

import glob
import os
from dataclasses import dataclass, field, replace
from typing import Optional

from .config import resolve_config_value


# ---------------------------------------------------------------------------
# Mk calendars: map (year, month) → Mk version for a source. Global has a real
# multi-Mk timeline; the others are single-Mk or Mk-less for now.
# ---------------------------------------------------------------------------

def _mk_um_global(year, month):
    """Global UM Mk calendar (delegates to the canonical mapping in iris_io)."""
    from .iris_io import get_Mk
    return get_Mk(year, month)


def _mk_um1p5km(year, month):
    """
    UM 1.5 km UK LimitedArea Mk calendar.

    Only Mk4 (from 2017-07) is confirmed present in the archive. Mk2/Mk3 cover
    earlier periods but their exact date boundaries are not yet known.
    """
    month = int(month)
    if year > 2017 or (year == 2017 and month >= 7):
        return 4
    raise ValueError(
        f"UM1p5km Mk calendar for {year}-{month:02d} is not yet defined "
        f"(only Mk4 from 2017-07 is confirmed; Mk2/Mk3 boundaries TBD)."
    )


def _mk_none(year, month):
    """Source has no Mk in its paths/filenames (e.g., NZCSM)."""
    return None


_MK_CALENDARS = {
    "um_global": _mk_um_global,
    "um1p5km": _mk_um1p5km,
    "none": _mk_none,
}


# ---------------------------------------------------------------------------
# Region schemes: map a scheme name → {region_id: [min_lat, max_lat, min_lon,
# max_lon]} in true lat/lon, or None for single-file (non-tiled) sources.
# ---------------------------------------------------------------------------

def _regions_world14():
    from .regions import get_saved_region_bounds
    return get_saved_region_bounds()


def _regions_uk16():
    raise NotImplementedError(
        "uk16 region bounds are not generated yet. The 16 UM1p5km UK tiles are on "
        "a rotated-pole grid; run the (generalised) make-native-grid for UM1p5km to "
        "derive and save their true-lat/lon bounds under data/UM1p5km/."
    )


_REGION_SCHEMES = {
    "world14": _regions_world14,
    "uk16": _regions_uk16,
    "none": lambda: None,
}


# ---------------------------------------------------------------------------
# Variable groups: which cubes to extract and how each is gridded. Selected by
# (name, want_levels) — see met_extract.extract._pick. The defaults suit the UM
# 'I' set; sources override where cube names differ.
#   mass_vars              — already on the mass grid (instantaneous)
#   wind_vars              — staggered instantaneous winds (u, v); interp/rotate to mass
#   averaged_mass_vars     — time-mean surface fields on the mass grid
#   averaged_staggered_vars— time-mean staggered surface fields (regular-grid sources)
# ---------------------------------------------------------------------------

_DEFAULT_MASS_VARS = (
    ("air_pressure", True),
    ("air_pressure_at_sea_level", False),
    ("air_temperature", True),
    ("atmosphere_boundary_layer_thickness", False),
    ("specific_humidity", True),
    ("surface_air_pressure", False),
    ("upward_air_velocity", True),
)
_DEFAULT_WIND_VARS = (("x_wind", True), ("y_wind", True))
_DEFAULT_AVERAGED_MASS_VARS = ("surface_upward_sensible_heat_flux",)
_DEFAULT_AVERAGED_STAGGERED_VARS = (
    "atmosphere_downward_eastward_stress",
    "atmosphere_downward_northward_stress",
)


# ---------------------------------------------------------------------------
# The source descriptor
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MetSource:
    """
    Declarative description of one met data type.

    Attributes
    ----------
    name : str
        Registry key (e.g. 'UM_Global').
    archive_directory : str
        Base directory of the archive (may contain a `{user}` template; resolved
        by :func:`get_source`).
    path_template : str
        Archive-relative glob template for one region/time, with placeholders
        `{mk} {date} {region} {vs} {nlev} {ext}`. Unused placeholders are ignored.
        `{date}` is a glob key (e.g. '201601' or '20150701'); a trailing `*` in
        the template matches the sub-daily time portion.
    grid_type : str
        'regular' (lat/lon) or 'rotated_pole' (grid_latitude/longitude + a pole).
    region_scheme : str
        'world14' | 'uk16' | 'none' — which region-bounds table applies.
    tiled : bool
        True if a domain spans several region files that must be joined; False if
        one file already covers the whole domain (no join).
    n_levels : int
        Number of model levels in the source (L59 → 59, L57 → 57, L70 → 70).
    var_set : str
        Variable-set token in the filename ('I', 'M', or '' if none).
    cadence : str
        Nominal time step between files ('3h', '1h').
    compressed : str
        'false' (.pp), 'true' (.pp.gz), or 'by_mk' (.pp.gz for Mk<10 else .pp).
    mk_calendar : str
        Which Mk calendar in :data:`_MK_CALENDARS` maps dates → Mk.
    crosses_dateline : bool
        True if the domain straddles the 180° meridian (e.g. NZCSM).
    append_batch : int or None
        Preferred number of time steps to accumulate before each zarr append
        (used by the run loop). None = append per natural period (e.g. month).
        Small for big single-file sources so memory stays bounded.
    level_stride : int
        Level subsampling: keep level 1 plus every `level_stride`-th level.
    """

    name: str
    archive_directory: str
    path_template: str
    grid_type: str
    region_scheme: str
    tiled: bool
    n_levels: int
    var_set: str = ""
    cadence: str = "3h"
    compressed: str = "false"
    mk_calendar: str = "none"
    crosses_dateline: bool = False
    append_batch: Optional[int] = None
    level_stride: int = 3

    # Variable groups (see the module-level defaults above). Override per source
    # where cube names differ. deferred_vars documents fields intentionally not
    # yet extracted (e.g. NZCSM stresses, which need vector rotation).
    mass_vars: tuple = _DEFAULT_MASS_VARS
    wind_vars: tuple = _DEFAULT_WIND_VARS
    averaged_mass_vars: tuple = _DEFAULT_AVERAGED_MASS_VARS
    averaged_staggered_vars: tuple = _DEFAULT_AVERAGED_STAGGERED_VARS
    deferred_vars: tuple = ()

    # -- Mk / levels / regions ------------------------------------------------

    def load_vars(self):
        """Flat, de-duplicated list of cube names to pass to ``iris.load``."""
        names = [n for n, _ in self.mass_vars]
        names += [n for n, _ in self.wind_vars]
        names += list(self.averaged_mass_vars)
        names += list(self.averaged_staggered_vars)
        seen, out = set(), []
        for n in names:
            if n not in seen:
                seen.add(n)
                out.append(n)
        return out

    def get_mk(self, year, month):
        """Return the Mk version for a date, or None for Mk-less sources."""
        return _MK_CALENDARS[self.mk_calendar](year, month)

    def levels(self):
        """Model levels to load: level 1 plus every `level_stride`-th, ≤ n_levels."""
        return [1] + list(range(1, self.n_levels + 1))[self.level_stride - 1::self.level_stride]

    def region_bounds(self):
        """Region-bounds dict for this source's scheme, or None if not tiled."""
        return _REGION_SCHEMES[self.region_scheme]()

    def region_ids(self):
        """Sorted region IDs for this source, or None if not tiled."""
        bounds = self.region_bounds()
        return sorted(bounds) if bounds else None

    # -- filename construction ------------------------------------------------

    def _var_set_token(self, mk):
        """The `{vs}` token, handling the Global-Mk6 quirk (no `_I_` in Mk6 files)."""
        if not self.var_set:
            return ""
        if self.mk_calendar == "um_global" and mk == 6:
            return ""
        return f"{self.var_set}_"

    def _ext(self, mk):
        """File extension, honouring the compression policy."""
        if self.compressed == "true":
            return ".pp.gz"
        if self.compressed == "by_mk":
            return ".pp.gz" if (mk is not None and mk < 10) else ".pp"
        return ".pp"

    def glob_pattern(self, date, region=None, mk=None):
        """Build the absolute glob pattern for one (date, region)."""
        rel = self.path_template.format(
            mk=mk,
            date=date,
            region="" if region is None else region,
            vs=self._var_set_token(mk),
            nlev=self.n_levels,
            ext=self._ext(mk),
        )
        return os.path.join(self.archive_directory, rel)

    def list_files(self, date, region=None, mk=None):
        """
        Return the sorted list of archive files matching a (date, region).

        Parameters
        ----------
        date : str
            Glob key: 'YYYYMM' (month), 'YYYYMMDD' (day), etc.
        region : int, optional
            Region ID (required for tiled sources; ignored otherwise).
        mk : int, optional
            Mk version (required where the path/filename includes `{mk}`).
        """
        if self.tiled and region is None:
            raise ValueError(f"source {self.name!r} is tiled; a region is required")
        return sorted(glob.glob(self.glob_pattern(date, region=region, mk=mk)))


# ---------------------------------------------------------------------------
# Built-in source registry
# ---------------------------------------------------------------------------

SOURCES = {
    "UM_Global": MetSource(
        name="UM_Global",
        archive_directory="/gws/ssde/j25a/name/met_archive/Global/",
        path_template="UMG_Mk{mk}PT/MO{date}*.UMG_Mk{mk}_{vs}L{nlev}PT{region}{ext}",
        grid_type="regular",
        region_scheme="world14",
        tiled=True,
        n_levels=59,
        var_set="I",
        cadence="3h",
        compressed="by_mk",
        mk_calendar="um_global",
        crosses_dateline=False,
        append_batch=None,   # append per month (current behaviour)
    ),
    "UM1p5km": MetSource(
        name="UM1p5km",
        archive_directory="/gws/ssde/j25a/name/met_archive/LimitedArea/",
        path_template="UM1p5km_Mk{mk}PT/PT{region}/MO{date}*.UM1p5km_Mk{mk}_{vs}L{nlev}PT{region}{ext}",
        grid_type="rotated_pole",
        region_scheme="uk16",
        tiled=True,
        n_levels=57,
        var_set="I",
        cadence="1h",
        compressed="false",
        mk_calendar="um1p5km",
        crosses_dateline=False,
    ),
    "NZCSM": MetSource(
        name="NZCSM",
        archive_directory="",   # TBD — set via config data_types.NZCSM.archive_directory
        path_template="name_nzcsm_{date}*.pp",
        grid_type="rotated_pole",
        region_scheme="none",
        tiled=False,            # single file over the whole domain → no join
        n_levels=70,
        var_set="",
        cadence="1h",
        compressed="false",
        mk_calendar="none",
        crosses_dateline=True,  # NZ domain straddles 180°
        append_batch=24,        # ~800 MB/step → append a day at a time
        # Stresses deferred: NZCSM's are `surface_downward_*_stress` (different
        # names from Global) AND are vectors on the rotated grid, so they need
        # rotate_winds-style rotation combined with the interval-end averaging —
        # the hardest case. Added in a follow-up; the rest of the 12 vars ship now.
        averaged_staggered_vars=(),
        deferred_vars=(
            "surface_downward_eastward_stress",
            "surface_downward_northward_stress",
        ),
    ),
}


def get_source(name, cfg=None):
    """
    Resolve a :class:`MetSource` by name, applying config overrides.

    Config may provide a ``data_types:`` block keyed by source name to override
    fields (most importantly ``archive_directory``, which can carry a ``{user}``
    template). For backward compatibility, ``UM_Global`` falls back to the
    top-level ``met_archive_directory`` when no explicit override is given.

    Parameters
    ----------
    name : str
        Source name (e.g., 'UM_Global').
    cfg : met_extract.config.Config, optional
        Configuration object used to resolve/override archive paths.

    Returns
    -------
    MetSource
    """
    if name not in SOURCES:
        raise ValueError(
            f"Unknown data type {name!r}. Available: {sorted(SOURCES)}"
        )
    src = SOURCES[name]

    if cfg is None:
        return src

    overrides = (cfg.get("data_types", {}) or {}).get(name, {}) or {}

    archive = overrides.get("archive_directory")
    if archive is None and name == "UM_Global":
        archive = cfg.get("met_archive_directory")  # back-compat
    if archive is None:
        archive = src.archive_directory
    archive = resolve_config_value(archive, cfg.data)

    changes = {"archive_directory": archive}
    # Allow a small set of scalar overrides from config (paths/tuning), not the
    # structural fields (grid_type, region_scheme, tiled) which are code-defined.
    for key in ("append_batch", "level_stride", "var_set", "cadence"):
        if key in overrides:
            changes[key] = overrides[key]

    return replace(src, **changes)


def list_sources():
    """Return the names of all registered built-in sources."""
    return sorted(SOURCES)
