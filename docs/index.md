# extract_um_met

Extract meteorology from the Met Office Unified Model (UM) — iris `.pp` files
hosted on JASMIN — into analysis-ready yearly [Zarr](https://zarr.dev/) stores.

`extract_um_met` is a single command-line tool that extracts each world region,
joins them into your domain, and appends the result to a yearly store, with
support for multiple data types (global UM, limited-area, convective-scale),
three grid modes, and CF/provenance metadata.

```bash
python -m extract_um_met run --domain SA --date 2016
# → {zarr_save_directory}/SOUTHAMERICA/SOUTHAMERICA_Met_2016.zarr
```

## Quick start

```bash
cd extract_um_met
module purge && module load jaspy                       # JASMIN Python env
export TMPDIR=/work/scratch-pw5/$USER/tmp && mkdir -p "$TMPDIR"
cp config.example.yaml config.yaml                      # then edit your copy
python -m extract_um_met make-native-grid --sample-date 201601   # one-off
python -m extract_um_met run --domain SA --date 20160115   # single-day smoke test
```

`config.yaml` is git-ignored (per-user paths). The shared structure — domains,
data-type archive paths, grid specs — lives in `config.example.yaml`.

## Documentation

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} Setup
:link: how_to_setup
:link-type: doc

Environment (jaspy), `TMPDIR`, writing `config.yaml`, native grids.
**Start here.**
:::

:::{grid-item-card} Data types & grids
:link: how_to_datatypes
:link-type: doc

The data types, Mk blocks & resolutions, world regions, grid modes, variables.
:::

:::{grid-item-card} Extracting
:link: how_to_extract
:link-type: doc

The CLI: `run` / `extract` / `make-native-grid`, options, resume, SLURM,
adding a domain.
:::

:::{grid-item-card} Codebase
:link: how_to_code
:link-type: doc

Module map — what each module does and how the pipeline flows.
:::

::::

## At a glance

- **Data types:** `UM_Global` (global UM, Mk6–11), `UM1p5km` (1.5 km UK
  limited-area), `NZCSM` (New Zealand convective-scale). Only the archive path
  is per-environment; structural properties are code-defined in `sources.py`.
- **Availability:** Global Mk6–Mk9 (2011-01 … 2017-06) are extractable today;
  Mk10/11 are symlinked to `/badc/` and not yet accessible.
- **Grid modes:** `footprint` (default), `regular`, `native` — set per domain in
  config.
- **Output:** one yearly Zarr store per domain, dims `time × levels × lat × lon`,
  float32 + zstd, CF + provenance attributes.

```{toctree}
:hidden:
:maxdepth: 2

how_to_setup
how_to_datatypes
how_to_extract
how_to_code
roadmap
```
