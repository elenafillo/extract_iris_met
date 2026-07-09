# met_extract Usage & Development Guide

This document describes the refactored `met_extract` package: its structure, current capabilities, usage patterns, and implementation roadmap.

## Overview

`met_extract` is a unified CLI tool that extracts UM meteorology data from `.pp` files into yearly Zarr stores. The refactoring consolidates three disjoint scripts (extraction, joining, conversion) into one workflow with support for custom and native grids, dependency pinning, and clearer provenance tracking.

**Key improvements over the original:**
- Single `python -m met_extract run` command (no hand-edited SLURM files)
- Yearly Zarr output by default (not monthly NetCDF)
- Three grid modes: footprint (default), regular (custom bounds), native (UM native grid)
- Native grids saved for reference and reuse
- Self-contained config (no external `gates` package dependency)
- Pinned environment (`environment.yml`)

---

## Package Structure

```
met_extract/
├── __init__.py                # Package metadata
├── __main__.py                # Entry point: python -m met_extract
├── config.py                  # Config loading & resolution
├── iris_io.py                 # Iris I/O (load_iris, get_Mk, remove_coord_callback)
├── regions.py                 # Region/domain grid functions
├── grid.py                    # ✓ Grid building & native grid I/O
├── extract.py                 # ✓ Per-region extraction
├── join.py                    # ✓ Monthly joining
├── metadata.py                # ✓ CF / ARCO metadata (coord + global attrs)
├── zarr_io.py                 # ✓ Zarr append, encoding & finalization
└── cli.py                     # ✓ CLI: run, extract, make-native-grid subcommands

scripts/                        # [TODO] SLURM templates
├── run_year.slurm
└── make_native_grid.slurm

data/                           # ✓ Native grids for Mk6–9 (created by make-native-grid)
└── native_grid_Mk{6..9}_*.nc

config.yaml                     # ✓ zarr_save_directory, zarr_format, native_grid_directory, metadata:
README.md                       # [TODO] Rewrite with new workflow
environment.yml                 # [TODO] Pin producer versions
usage.md                        # This file
```

---

## Current Capabilities

### ✓ Config Management
- Load and resolve `config.yaml` with `{user}` template expansion
- Access domain configs with fallback defaults
- Resolve domain by key (`SA`) or `domain_name` (`SOUTHAMERICA`)

### ✓ Iris Data I/O
- Load `.pp` and `.pp.gz` files with iris
- Automatic Mk-version detection (6–11) based on year/month
- Handle both compressed (Mk 6–9, 11) and uncompressed (Mk 10) data

### ✓ Region & Domain Grid Functions
- Get world region bounds (14 global regions)
- Find regions overlapping a bounding box
- Build domain-specific grids (trim global grid to domain regions)
- Deduplicate coordinates (for stitching)

### ✓ Native Grid Extraction & I/O
- Extract native UM lat/lon from iris cubes (no interpolation)
- Save native grids per Mk to `native_grid_Mk*.nc`
- Load saved native grids for reuse
- Build target grids in three modes:
  - **Footprint** (default): load reference footprint, pad by edge sizes
  - **Regular**: build `np.arange` mesh from bounds + resolution
  - **Native**: subset saved native grid (no interpolation)

### ✓ Per-Region Extraction (`extract.py`)
- `extract_region(domain_key, year, month, region_id, cfg, ...)` ports the
  per-region body of `satellite_met_1b1_fixed_v3.py`:
  - Loads the region's monthly UM data (`.pp`/`.pp.gz`) via `load_iris`
  - Selects 1-in-3 model levels (`LEVELS = [1, 3, 6, …, 57]`)
  - Aligns the staggered `x_wind`/`y_wind` onto the mass grid by interpolation
  - Normalises longitude to `[-180, 180]`, builds a real `time` dim
  - Footprint/regular → interpolates onto the target grid; native → passthrough
  - Slices to the region's bounds (handling dateline-crossing regions)
  - Writes a per-region NetCDF intermediate to `{scratch}/files/`
  - Cleans up unzipped `.pp` scratch after loading

### ✓ Monthly Joining (`join.py`)
- `join_month(domain_key, year, month, cfg, ...)` ports `satellite_met_join_v2.py`:
  - Ensures each region intermediate exists (extracts missing ones, reuses present)
  - Stitches per the domain region grid: concat along latitude within a longitude
    column, then concat columns along longitude
  - Normalises/dedupes coordinates, fills 1-cell join seams
  - Renames to the zarr schema (`lat`/`lon`/`levels`); drops only `sigma_0`/`level_height_0`
    (keeps `forecast_period`/`forecast_reference_time` as time-indexed provenance coords)
  - Stamps CF + `delta_lat`/`delta_lon` metadata (via `metadata.apply_cf_metadata`)
  - Returns a monthly `xr.Dataset` ready to append

### ✓ Zarr I/O (`zarr_io.py`)
- `append_month_to_year_store(month_ds, store, zarr_format, first)` — first month
  writes `mode="w"`, later months append along `time`; chunks `{time:1, lat:-1, lon:-1, levels:3}`
- On the first write, `_build_encoding` pins an explicit on-disk layout: float32 +
  NaN `_FillValue` + **zstd Blosc** compression for data variables, and CF
  `units`/`calendar` for `time` (zarr v2; v3 keeps library defaults)
- `finalize_attrs(store, extra_attrs)` — records provenance/completeness
  (`months_present`, `missing_months`, `year_complete`, `time_start/end`, `grid_mode`, …)

### ✓ CF / ARCO Metadata (`metadata.py`)
- `apply_cf_metadata(ds, cfg, ...)` makes each store self-describing (analysis-ready):
  - **Coordinate attrs** — CF `standard_name`/`units`/`axis` for `lat`/`lon`/`time`,
    a description + `positive` for `levels`, and `standard_name` for the forecast coords
  - **Global attrs** — `Conventions`, `title`, `source`, `history`, `comment`
    (documents the wind regridding + level subsampling), plus `institution`/`references`
    from the config `metadata:` block (empty fields omitted)
  - **Variable attrs** — iris `units`/`standard_name` preserved through the pipeline
    (extract/join run under `xr.set_options(keep_attrs=True)`)
- Cloud-optimized side (consolidated metadata, chunking, pinned compression) lives in `zarr_io.py`

### ✓ CLI: `run`, `extract`, `make-native-grid`

`make-native-grid` — extract and save native UM grids for one or more Mk versions:
```bash
python -m met_extract make-native-grid --mk 9 --sample-date 201601
python -m met_extract make-native-grid --dry-run  # All Mks, no write
python -m met_extract make-native-grid --output-dir /custom/path/
```

`run` — extract → join → append to a zarr store, with resume:
```bash
python -m met_extract run --domain SA --date 201601          # one month
python -m met_extract run --domain SA --date 2016            # full year
python -m met_extract run --domain SA --date 20160115        # single day → debug store
python -m met_extract run --domain SA --date 2016 --overwrite # rebuild
python -m met_extract run --domain SA --date 201601 --grid-mode native
python -m met_extract run --domain SA --date 201601 --dry-run
```
`--date` accepts `YYYY` (full year), `YYYYMM` (month), or `YYYYMMDD` (single day →
a standalone `{DOMAIN}_Met_{YYYYMMDD}.zarr` debug store, always a fresh write).
Other flags: `--zarr-format {2,3}`, `--keep-intermediates`.

`extract` — write per-region intermediates only (debugging/retry):
```bash
python -m met_extract extract --domain SA --date 201601            # all regions, whole month
python -m met_extract extract --domain SA --date 201601 --region 6 # one region
python -m met_extract extract --domain SA --date 20160115 --region 6 # one region, one day
```

**Resume semantics:** for an existing store (no `--overwrite`), months already
present are skipped and only later months are appended. Backfilling an *earlier*
missing month via append is not possible (time order must be preserved) — the run
warns and you rebuild with `--overwrite`.

### Mk availability
Mk 6–9 (2011-01 … 2017-06) are extractable. **Mk 10 and 11 are currently
inaccessible** (symlinked to `/badc/`), so `run`/`extract`/`make-native-grid`
will report missing files for dates ≥ 2017-07. The code paths for Mk 10 (already
uncompressed) and Mk 11 are kept intact for when access is restored.

---

## Usage Examples

### 1. Extract Native Grids

Before running extraction, save the native UM grids:

```bash
cd /home/users/elenafi/extract_iris_met

# Extract native grid for Mk 10
python -m met_extract make-native-grid --mk 10 --sample-date 201601

# Extract all Mk versions (6–11)
python -m met_extract make-native-grid --sample-date 201601

# Dry-run (no write)
python -m met_extract make-native-grid --dry-run
```

Output: `data/native_grid_Mk10.nc`, `data/native_grid_Mk9.nc`, etc.

### 2. Extract & Append Meteorology

The main workflow (extract → join → append per month):

```bash
# Extract a full year to zarr
python -m met_extract run --domain SA --date 2016

# Extract a single month
python -m met_extract run --domain SA --date 201601

# Resume (skip months already in the store)
python -m met_extract run --domain SA --date 2016

# Rebuild from scratch
python -m met_extract run --domain SA --date 2016 --overwrite

# Use custom grid mode (overrides config)
python -m met_extract run --domain SA --date 2016 --grid-mode native
```

### 3. Extract Per-Region Intermediates

For debugging or retrying flaky regions:

```bash
# Extract all regions for a domain/month
python -m met_extract extract --domain SA --date 201601

# Extract one region only
python -m met_extract extract --domain SA --date 201601 --region 6
```

---

## Configuration

Edit `config.yaml` in the repo root. Key sections:

### Top-level settings
```yaml
user: ""                              # Blank; filled from $USER
zarr_save_directory: "/path/{user}/satellite_met_zarr/"
zarr_format: 2                        # Or 3 (see Environment section of plan)
native_grid_directory: "data/"
met_archive_directory: "/gws/ssde/j25a/name/met_archive/Global/"
scratch_path: "/work/scratch-pw5/{user}/"
reference_footprints_directory: "/gws/ssde/j25b/acrg/elenafi/example_footprints"
```

### Metadata (CF / ARCO)
Written into every store's attributes. Fill in what applies; leave a field `""`
to omit it (it is not written as a blank attribute).
```yaml
metadata:
  conventions: "CF-1.10"     # CF conventions version claimed by the output
  institution: ""            # e.g. "University of Bristol, ACRG"
  references: ""             # DOI or URL documenting the dataset/method
  title: ""                  # optional; auto "UM meteorology, {DOMAIN} {year}" if empty
  source: ""                 # optional; auto from the UM Mk version if empty
  comment: ""                # optional; appended to the auto processing comment
```

### Domain configuration (grid modes)

Each domain can specify a grid mode (footprint, regular, or native):

```yaml
domains:
  SA:
    domain_name: "SOUTHAMERICA"
    world_regions_codes: [6, 9, 10, 13]
    
    # Grid mode: footprint (default, backward compatible)
    grid:
      mode: footprint
      footprint: "GOSAT-BRAZIL-column_SOUTHAMERICA_201801.nc"
      edge_size_lat: [100, 100]
      edge_size_lon: [85, 100]
    
    # Alternatives (comment out above to use):
    # grid:
    #   mode: regular
    #   lat_bounds: [-25, 15]
    #   lon_bounds: [-75, -30]
    #   dlat: 0.234
    #   dlon: 0.352
    #
    # grid:
    #   mode: native
    #   lat_bounds: [-25, 15]
    #   lon_bounds: [-75, -30]
    #   native_grid: majority  # or "latest", "Mk10", etc.
```

If a domain has no `grid:` block, it defaults to footprint mode with the domain's `footprint` key.

---

## Data Flow (as implemented)

```
python -m met_extract run --domain SA --date 2016
        │
        ├─→ resolve config, domain, date → 2016-01 through 2016-12
        │
        └─→ for each month:
            ├─→ extract each world region  (keep_attrs=True)
            │   ├─ load iris from .pp(.gz)  (name-based cube selection)
            │   ├─ align winds onto the mass grid
            │   ├─ build time dim; retain forecast_period/forecast_reference_time
            │   │   (guard: each timestamp must be a unique frt+period pair)
            │   ├─ footprint/regular → interpolate onto target grid; native → passthrough
            │   ├─ slice to region bounds (dateline-aware)
            │   └─ save per-region NetCDF intermediate to scratch (bounds peak memory)
            │
            ├─→ join regions  (keep_attrs=True)
            │   ├─ concat latitudes (within longitude columns), then longitudes
            │   ├─ fill 1-cell seams
            │   ├─ rename: latitude→lat, longitude→lon, model_level_number→levels
            │   ├─ drop only level_height_0, sigma_0
            │   ├─ add delta_lat/delta_lon; stamp CF + config metadata
            │   └─ return monthly domain xr.Dataset
            │
            └─→ append to yearly zarr store
                ├─ month 1 → to_zarr(mode="w") with pinned encoding (float32, zstd, CF time)
                ├─ months 2–12 → to_zarr(append_dim="time")
                └─ finalize provenance/completeness attrs (consolidated)

OUTPUT: {zarr_save_directory}/SOUTHAMERICA/SOUTHAMERICA_Met_2016.zarr
```

---
### Future to-dos

**✓ Done**
- ✓ **ARCO / CF metadata** — `metadata.py` + a `metadata:` block in `config.yaml`
  (Conventions, title, institution, references, source, comment; CF coordinate
  attrs; iris variable `units`/`standard_name` preserved via `keep_attrs`; pinned
  zstd Blosc compression + CF `time` encoding). Empty config fields are omitted.
- ✓ **Retain `forecast_reference_time(time)` and `forecast_period(time)`** as
  time-indexed provenance coordinates — with a guard in `extract.py` that raises
  if the `(reference_time, period)` combination is not unique per timestamp
  (would otherwise give duplicate/ambiguous timesteps).
- ✓ **`delta_lat` / `delta_lon` global attributes** (mean grid spacing, from `join.py`).
- ✓ **Single-day extraction** (`--date YYYYMMDD`, bonus) — loads just that day's
  ~8 files per region into a standalone `{DOMAIN}_Met_{YYYYMMDD}.zarr` debug store,
  a cheap end-to-end smoke test.

**Still to do**
- Add option in config for multiple folders/data types, eg `/gws/ssde/j25a/name/met_archive/Global/` has Mk folders, but I also want to explore and enable access to 4m res data at `/gws/ssde/j25a/name/met_archive/LimitedArea/UM1p5km_Mk{Mk}PT/` (which has options `UM1p5km_Mk2PT`,`UM1p5km_Mk3PT`, `UM1p5km_Mk4PT`, then has folders for each region `PT10` etc). The grids data, years covered, resolution etc should be saved under a folder in `data` with the same reference name as the data type (ge `Global`)
- Add a dedicated single-region path (extract → setup, skip the join) for domains
  that map to one world region. (`join_month` already handles a 1-region domain as
  a trivial concat, but a direct path would avoid the join machinery.)
- **Native-grid cropping gap:** in `native` mode, extraction passes each region
  through at its *full* region extent (sliced only to region bounds), so it does
  **not** yet crop to a domain bounding box — `lat_bounds`/`lon_bounds` in a native
  `grid:` block currently affect only the canonical target grid, not the extracted
  output. Needs a final crop to those bounds so a native domain matches its
  footprint-mode counterpart in size.
---


## Next Steps (Implementation Roadmap)

### Phase 1: Core Extraction & Joining ✓ DONE

All of `extract.py`, `join.py`, `metadata.py`, `zarr_io.py`, and the
`run`/`extract` CLI subcommands are implemented and wired together. Phase-1
hardening is done:

- **[done] Name-based cube selection** — variables are selected by name +
  level-dimension presence (`_pick` + `MASS_VARS`/`WIND_VARS`), raising on any
  missing/ambiguous match rather than trusting positional order. Verified to pick
  the identical cubes as the old `[0,1,3,4,5,7]`/`8`/`9` indices on Mk9. Preserves
  exact parity (still excludes `surface_upward_sensible_heat_flux` and the 1.5 m
  `air_temperature`). See "Known Constraints" below.
- **[done] CF/ARCO metadata, forecast provenance coords, delta attrs** — see
  "Future to-dos → Done".

<details><summary>Original Phase-1 plan (for reference)</summary>

**1.1 Implement `extract.py` — Per-region extraction**
- Move logic from `satellite_met_1b1_fixed_v3.py:192-303` (per-region body)
- Function: `extract_region(domain, year, month, region_id, cfg) → Path`
- Call `build_target_grid()` from `grid.py` to get target lat/lon
- Use `.interp()` or `.sel()` depending on grid mode and native Mk resolution
- Save per-region zarr to scratch (e.g., `SA_Met_201601_region6.zarr`)

**1.2 Implement `join.py` — Monthly joining**
- Move logic from `satellite_met_join_v2.py:122-228` (concat logic)
- Function: `join_month(domain, year, month, cfg) → xr.Dataset`
- Call `extract_region()` for each region in the domain
- Concat regions (seam-filling per `satellite_met_join_v2.py:29-43`)
- Apply `_preprocess` transform (rename, drop, deduplicate)
- Return dask-lazy Dataset ready for zarr append

**1.3 Implement `zarr_io.py` — Zarr append & finalization**
- Function: `append_month_to_year_store(month_ds, store_path, zarr_format, first)`
  - Chunk {time:1, lat:-1, lon:-1, levels:3}
  - `first=True` → `to_zarr(..., mode="w")`; else `to_zarr(..., append_dim="time")`
- Function: `finalize_attrs(store_path)` — reopen store, write provenance attrs, re-consolidate
- Provenance attrs: created_by, created_at, months_present, missing_months, year_complete, time_start/end, zarr_chunks, grid_mode, native_grid_info

**1.4 Implement `cli.py` — `run` subcommand**
- Parse `--domain`, `--date {YYYY|YYYYMM|all}`, `--overwrite`, `--dry-run`, `--grid-mode`
- Resolve domain (by key or domain_name)
- Loop months, call `join_month()` → `append_month_to_year_store()` → `finalize_attrs()`
- Resume logic: if store exists, read `time` coord, skip present months

**1.5 Implement `cli.py` — `extract` subcommand**
- Parse `--domain`, `--date {YYYYMM}`, `--region` (optional)
- Call `extract_region()` directly for specified region(s)

**Estimated effort:** ~2–3 full implementations (extract + join are the bulk; zarr_io is glue).

</details>

### Phase 2: Documentation & Deployment

**2.1 Rewrite `README.md`**
- New one-command flow
- Config schema (grid modes)
- How to add a domain
- How to regenerate native grids
- JASMIN specifics (module load, queue)

**2.2 SLURM templates**
- `scripts/run_year.slurm` — array over (domain, year)
- `scripts/make_native_grid.slurm` — standalone for grid generation

**2.3 `environment.yml`**
- Pin: Python 3.12, xarray=2025.7.0, dask=2025.5.1, zarr=3.0.10, iris=3.12.2, numpy=2.2.6

**2.4 Clean up old files**
- Delete: `data_utils/convert_met_to_zarr.py`, `satellite_met_join_v2 copy.py`, `Untitled.ipynb`, `run_single_job.txt`, `run_parallel_job.txt`, `empty_test_job.txt` [these will get deleted by the user with `git rm --cached`]
- Reduce to shim: `met_functions.py`, `satellite_met_1b1_fixed_v3.py`, `satellite_met_join_v2.py` (or remove once callers are updated)

### Phase 3: Testing & Validation

**3.1 Single-month smoke test**
- Run `python -m met_extract run --domain SA --date 201601` on a small domain
- Open the store: `xr.open_zarr(path)` — check dims, vars, `time` coverage, provenance attrs

**3.2 Append correctness**
- Run `--date 2016`, confirm all 12 months
- Re-run without `--overwrite`, confirm skip (resume works)
- Re-run with `--overwrite`, confirm rebuild

**3.3 Format/round-trip**
- Open produced `zarr_format=2` store from zarr 2.18.7 env AND zarr 3.0.10 env
- Confirm both read it

**3.4 Value parity**
- For one (domain, month) with known-good monthly NetCDF, compare variables/levels

**3.5 Grid modes**
- Extract same small domain in footprint vs native mode
- Confirm native produces native lat/lon with no interp; footprint matches today's grid

**3.6 SLURM deployment**
- Submit `scripts/run_year.slurm` for one (domain, year)
- Confirm Dask cluster sizes from SLURM env
- Confirm store lands in zarr_save_directory

---

## Known Constraints & Assumptions

### Variable/cube selection (name-based)
`extract.py` selects cubes by name + level-dimension presence via `_pick`, driven
by `MASS_VARS`/`WIND_VARS`, and raises if a name matches zero or multiple cubes.
On Mk9 (region 6) the 10 loaded cubes are, in order: `air_pressure`(L),
`air_pressure_at_sea_level`, `air_temperature`(1.5 m), `air_temperature`(L),
`atmosphere_boundary_layer_thickness`, `surface_air_pressure`,
`surface_upward_sensible_heat_flux`, `upward_air_velocity`(L), `x_wind`(L),
`y_wind`(L). The kept set is the 6 mass vars + 2 winds; by decision it **excludes**
the 1.5 m `air_temperature` and `surface_upward_sensible_heat_flux` (exact parity
with the original script). Because selection is by name, a changed archive or iris
ordering now raises a clear error instead of silently mis-selecting.

### Mk Resolution Changes
- Native grid resolution changes between some Mk versions (mid-year boundaries: 2013-05, 2014-07, 2015-08, 2017-07, 2022-06)
- **Decision:** Native-mode extraction snaps to one canonical native grid (default: majority Mk by month count) and interpolates only off-resolution months
- Footprint and regular modes are immune (always interpolate onto a fixed target grid)
- **Implementation check:** whether adjacent Mk native grids are nested (would allow `.sel()` for off-res months instead of interp)

### Zarr Format Compatibility
- Write `zarr_format=2` by default (readable by both zarr 2.18.7 and zarr 3.x)
- Switch to `3` only once all consumers are on zarr 3.x
- Downstream xarray/dask upgrade is optional; current compatibility achieved with v2 format

### Per-Region Intermediates
- Stored as **single-file NetCDF** in `{scratch}/files/` (transient; deleted after
  the month appends unless `--keep-intermediates`). NetCDF — not zarr — is
  deliberate here: one file per region is cheaper on shared scratch (fewer inodes),
  easy to inspect/delete, and read lazily by the join. Their purpose is to bound
  peak memory (spill one region, free it, load the next) rather than to be a product.

---

## Troubleshooting & Notes

### Run `make-native-grid` first
Before running the main `run` command, ensure native grids exist:
```bash
python -m met_extract make-native-grid --sample-date 201601
# Creates data/native_grid_Mk6.nc, ..., native_grid_Mk11.nc
```

### Check config paths
Ensure these directories exist or are writable:
- `scratch_path` (per-region intermediates)
- `zarr_save_directory` (final stores)
- `native_grid_directory` (saved grids)

### Dry-run before submitting SLURM
Test locally first:
```bash
python -m met_extract run --domain SA --date 201601 --dry-run
```

### Environment
The package requires:
- Python 3.10+ (tested with 3.12)
- iris ≥ 3.0
- xarray ≥ 2025.1.2
- dask
- zarr
- pyyaml

See `environment.yml` (once created) for exact pinned versions.

---

## Summary

**What's ready:**
- ✓ Package structure & module imports
- ✓ Config loading & resolution
- ✓ Iris I/O utilities
- ✓ Region/domain grid functions
- ✓ Native grid extraction & I/O
- ✓ Grid mode builder (all three modes)
- ✓ CLI scaffold with `make-native-grid` command

**What's next:**
1. Extract logic (`extract.py`)
2. Join logic (`join.py`)
3. Zarr I/O (`zarr_io.py`)
4. Main `run` command (`cli.py`)
5. Documentation & templates
6. Testing & validation

The foundation is solid and extensible. Each next step is a straightforward port of existing logic into the new module structure.
