import os
import re
import yaml
from collections import defaultdict, Counter
from datetime import datetime


# Load domain configuration from yaml file
def load_domains_config(yaml_path):
    with open(yaml_path, 'r') as f:
        config = yaml.safe_load(f)
    expected = {}
    for key, val in config['domains'].items():
        domain = val['domain_name']
        expected[domain] = val['world_regions_codes']
    return expected

# Extract actual files present in directory
def extract_present_files(files_dir):
    found_files = defaultdict(lambda: defaultdict(set))
    file_pattern = re.compile(r'(\w+)_Met_(\d{6})_(\d+)\.nc')
    for filename in sorted(os.listdir(files_dir)):
        match = file_pattern.match(filename)
        if match:
            region, yearmonth, file_number = match.groups()
            found_files[region][yearmonth].add(int(file_number))
    return found_files

# Identify missing files for each world region and month
def find_missing_files(found_files, expected_codes):
    missing_report = []
    for region in found_files:
        for yearmonth in found_files[region]:
            expected = set(expected_codes.get(region, []))
            found = found_files[region][yearmonth]
            missing = expected - found
            if missing:
                missing_report.append((region, yearmonth, sorted(missing)))
    return missing_report

# Identify months where all expected files are present
def find_complete_months(found_files, expected_codes):
    complete_months = []
    for region in found_files:
        for yearmonth in found_files[region]:
            expected = set(expected_codes.get(region, []))
            found = found_files[region][yearmonth]
            if expected == found:
                complete_months.append((region, yearmonth))
    return complete_months

# Summarise complete and missing months per region
def summarise_completeness(missing_files, complete_months):
    summary = defaultdict(lambda: Counter({"complete": 0, "missing": 0}))
    for region, _ in complete_months:
        summary[region]["complete"] += 1
    for region, _ , _ in missing_files:
        summary[region]["missing"] += 1
    return summary

# Summarise complete months by year
def summarise_complete_by_year(complete_months):
    yearly_summary = defaultdict(set)
    for region, yearmonth in complete_months:
        year = yearmonth[:4]
        yearly_summary[(region, year)].add(yearmonth)
    complete_sets = defaultdict(set)
    for (region, year), months in yearly_summary.items():
        if len(months) == 12:
            complete_sets[region].add(year)
    return complete_sets





yaml_config_path = "config.yaml"
# Open config file
with open(yaml_config_path, "r") as f:
    config = yaml.safe_load(f)
files_dir = os.path.join(config.get("scratch_path", ""), "files")

# Load expected region codes
expected_codes = load_domains_config(yaml_config_path)

# Parse found files from actual directory
found_files = extract_present_files(files_dir)

# Compare and report
missing_files = find_missing_files(found_files, expected_codes)
for region, yearmonth, missing in missing_files:
    print(f"Missing files for {region} {yearmonth}: {missing}")

complete_months = find_complete_months(found_files, expected_codes)
for region, yearmonth in complete_months:
    print(f"Complete set found for {region} {yearmonth}")

# Summary report per region
summary = summarise_completeness(missing_files, complete_months)
print("\nSummary:")
for region, counts in summary.items():
    print(f"{region}: {counts['complete']} complete, {counts['missing']} missing")

# Summary report by year
complete_years = summarise_complete_by_year(complete_months)
print("\nComplete years:")
for region, years in complete_years.items():
    for year in sorted(years):
        print(f"COMPLETE SET FOR {region} {year}")

# Save output to file with datetime
now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = f"logs/all_regions_summary_report_{now_str}.txt"
with open(output_path, "w") as f:
    f.write("\nComplete years:\n")
    for region, years in complete_years.items():
        for year in sorted(years):
            f.write(f"COMPLETE SET FOR {region} {year}\n")
            
    f.write("\nSummary per region:\n")
    for region, counts in summary.items():
        f.write(f"{region}: {counts['complete']} complete, {counts['missing']} missing\n")
    
    f.write("Missing files:\n")
    for region, yearmonth, missing in missing_files:
        f.write(f"Missing files for {region} {yearmonth}: {missing}\n")

    f.write("\nComplete months:\n")
    for region, yearmonth in complete_months:
        f.write(f"Complete set found for {region} {yearmonth}\n")

print(f"\nReport saved to: {output_path}")