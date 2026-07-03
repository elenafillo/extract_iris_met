import os
import re
import yaml
from collections import defaultdict, Counter
from datetime import datetime


def resolve_config_value(value, config):
    if not isinstance(value, str):
        return value
    try:
        return value.format(**config)
    except KeyError:
        return value

# Load domain configuration from yaml file
def load_domains_config(yaml_path):
    """
    Load domain configuration from the YAML file.

    Extracts:
      - domain_name (e.g. "CHINA")
      - world_regions_codes (expected chunk indices, e.g. [3,4,7,8])

    Parameters
    ----------
    yaml_path : str
        Path to the config.yaml file.

    Returns
    -------
    dict
        Dictionary of form:
            expected[region] = [list of expected chunk codes]
    """
    with open(yaml_path, 'r') as f:
        config = yaml.safe_load(f)

    expected = {}
    for key, val in config["domains"].items():
        region = val["domain_name"]
        expected[region] = val["world_regions_codes"]

    return expected

# Extract actual files present in directory
def extract_present_files_from_scratch(scratch_dir):
    """
    Scan the scratch directory for intermediate region chunk files.

    File format:
        <REGION>_Met_<YYYYMM>_<chunk>.nc

    Example:
        CHINA_Met_201701_3.nc

    Parameters
    ----------
    scratch_dir : str
        Flat directory containing intermediate chunk files.

    Returns
    -------
    dict
        found[region][yearmonth] = {chunk_numbers}
    """
    print(f"\nScanning the scratch directory for intermediate saved files:")
    print(f"  {scratch_dir}")
    found = defaultdict(lambda: defaultdict(set))
    pattern = re.compile(r"(\w+)_Met_(\d{6})_(\d+)\.nc")

    if not os.path.isdir(scratch_dir):
        print(f"  WARNING: {scratch_dir} directory not found; treating as no intermediate files present.")
        return found

    for filename in os.listdir(scratch_dir):
        match = pattern.match(filename)
        if match:
            region, yearmonth, chunk = match.groups()
            found[region.upper()][yearmonth].add(int(chunk))

    return found

def extract_present_files_from_final(final_dir):
    """
    Recursively scan the final joined-file directory for merged monthly files.

    Final file format:
        <REGION>_Met_<YYYYMM>.nc

    Region folders:
        China/, India/, Northafrica/, SouthAmerica/

    Parameters
    ----------
    final_dir : str
        Directory containing region subfolders and final files.

    Returns
    -------
    dict
        found[region][yearmonth] = {"FINAL"}
    """
    print(f"\nScanning the final met saving directory for joined monthly files:")
    print(f"  {final_dir}")
    found = defaultdict(lambda: defaultdict(set))
    pattern = re.compile(r"(\w+)_Met_(\d{6})\.nc")
    region_folders = {"china", "india", "northafrica", "southamerica"}

    for root, dirs, files in os.walk(final_dir):
        folder = os.path.basename(root).lower()
        region_from_folder = folder.upper() if folder in region_folders else None

        for filename in files:
            match = pattern.match(filename)
            if match:
                region_tag, yearmonth = match.groups()
                region = region_from_folder if region_from_folder else region_tag.upper()
                found[region][yearmonth].add("FINAL")

    return found

# Identify missing files for each world region and month
def find_missing_files(found_files, expected_codes):
    """
    Determine missing intermediate chunk files to help identify gaps in
    data coverage which need filling.

    If a FINAL file exists for a month, missing checks are skipped entirely.

    Parameters
    ----------
    found_files : dict
        Combined scratch + final file dictionary.

    expected_codes : dict
        Expected chunk indices per region.

    Returns
    -------
    list of (region, yearmonth, missing_codes)
    """
    missing = []

    for region in found_files:
        for yearmonth in found_files[region]:
            files = found_files[region][yearmonth]

            # FINAL overrides all scratch logic
            if "FINAL" in files:
                continue

            expected = set(expected_codes.get(region, []))
            found_chunks = {n for n in files if isinstance(n, int)}
            missing_codes = sorted(expected - found_chunks)

            if missing_codes:
                missing.append((region, yearmonth, missing_codes))

    return missing

# Identify months where all expected files are present
def find_complete_months(found_files, expected_codes):
    """
    Determine complete months for which we have all expected region files.

    Complete if:
      - FINAL file exists, OR
      - All expected region chunk codes are present.

    Parameters
    ----------
    found_files : dict
        Combined dictionary of scratch + final files.

    expected_codes : dict
        Expected chunk indices per region.

    Returns
    -------
    list of (region, yearmonth)
        All complete (region, month) combinations.
    """
    complete = []

    for region in found_files:
        for yearmonth in found_files[region]:
            files = found_files[region][yearmonth]

            if "FINAL" in files:
                complete.append((region, yearmonth))
                continue

            expected = set(expected_codes.get(region, []))
            found_chunks = {n for n in files if isinstance(n, int)}

            if found_chunks == expected:
                complete.append((region, yearmonth))

    return complete

# Summarise complete and missing months per region
def summarise_completeness(missing, complete):
    """
    Summarise number of complete and missing months per region.

    Parameters
    ----------
    missing : list
        Missing entries from find_missing_files().

    complete : list
        Complete entries from find_complete_months().

    Returns
    -------
    dict
        summary[region]["complete"]
        summary[region]["missing"]
    """
    summary = defaultdict(lambda: Counter({"complete": 0, "missing": 0}))

    for region, _ in complete:
        summary[region]["complete"] += 1

    for region, _, _ in missing:
        summary[region]["missing"] += 1

    return summary

# Summarise complete months by year
def summarise_complete_years(complete):
    """
    Identify years where all 12 months are complete, ie either are already joined
    or have all required separate region files.

    Parameters
    ----------
    complete : list of (region, yearmonth)

    Returns
    -------
    dict
        complete_years[region] = {years where all 12 months exist}
    """
    grouped = defaultdict(set)

    for region, ym in complete:
        year = ym[:4]
        grouped[(region, year)].add(ym)

    full_years = defaultdict(set)
    for (region, year), months in grouped.items():
        if len(months) == 12:
            full_years[region].add(year)

    return full_years



yaml_path = "config.yaml" if os.path.exists("config.yaml") else "config.yml"
# Open config file
with open(yaml_path, "r") as f:
    config = yaml.safe_load(f)

if not config.get("user"):
    config["user"] = os.environ.get("USER", "")

scratch_dir = os.path.join(resolve_config_value(config["scratch_path"], config), "files")
final_dir = resolve_config_value(config["met_save_directory"], config)

print("\n=== DIRECTORIES BEING SCANNED ===")
print(f"Scratch (intermediate chunks): {scratch_dir}")
print(f"Final (joined domain files):   {final_dir}")


# Load expected sandbox chunk codes
expected_codes = load_domains_config(yaml_path)

# Scan both locations
scratch_files = extract_present_files_from_scratch(scratch_dir)
final_files = extract_present_files_from_final(final_dir)

# Merge (FINAL always overrides scratch)
found = scratch_files
for region in final_files:
    for ym in final_files[region]:
        found[region][ym].update(final_files[region][ym])

print("\n=== FILES FOUND PER REGION ===")
for region in sorted(found):
    print(f"{region}: {sorted(found[region].keys())}")

# Compute completeness
missing = find_missing_files(found, expected_codes)
complete = find_complete_months(found, expected_codes)

# Year completeness
complete_years = summarise_complete_years(complete)
print("\n=== COMPLETE YEARS ===")
for region, years in complete_years.items():
    for y in sorted(years):
        print(f"{region} {y} COMPLETE")

# Report missing chunk files
print("\n=== MISSING CHUNK FILES ===")
for region, ym, missing_codes in sorted(missing, key=lambda x: (x[0], x[1])):
    print(f"{region} {ym}: missing {missing_codes}")

# Report complete months
print("\n=== COMPLETE MONTHS (ONLY YEARS THAT ARE NOT FULLY COMPLETE) ===")
for region, ym in sorted(complete, key=lambda x: (x[0], x[1])):
    year = ym[:4]
    # Print only if this year is NOT a fully complete year
    if year not in complete_years.get(region, set()):
        print(f"{region} {ym}")

# Region summary
summary = summarise_completeness(missing, complete)
print("\n=== SUMMARY PER REGION ===")
for region, counts in summary.items():
    print(f"{region}: {counts['complete']} complete, {counts['missing']} missing")


# Save report
now = datetime.now().strftime("%Y%m%d_%H%M%S")
report_path = f"logs/all_regions_summary_report_{now}.txt"

os.makedirs("logs", exist_ok=True)

with open(report_path, "w") as f:
    f.write("Scanning the final met saving directory for joined monthly files:\n")
    f.write(f"  {final_dir}\n\n")
    f.write("COMPLETE YEARS:\n")
    for region, years in complete_years.items():
        for year in sorted(years):
            f.write(f"{region} {year} COMPLETE\n")


    f.write("\nCOMPLETE MONTHS (ONLY YEARS THAT ARE NOT FULLY COMPLETE):\n")
    for region, ym in sorted(complete, key=lambda x: (x[0], x[1])):
        year = ym[:4]
        if year not in complete_years.get(region, set()):
            f.write(f"{region} {ym}\n")

    f.write("\nSUMMARY PER DOMAIN:\n")
    for region, counts in summary.items():
        f.write(f"{region}: {counts['complete']} complete, {counts['missing']} with missing regions\n")

    f.write("Scanning the scratch directory for saved intermediate files:\n")
    f.write(f"  {scratch_dir}\n\n")

    f.write("\nMISSING REGION FILES:\n")
    for region, ym, missing_codes in sorted(missing, key=lambda x: (x[0], x[1])):
        f.write(f"{region} {ym}: {missing_codes}\n")

print(f"\nReport saved to: {report_path}")