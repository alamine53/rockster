# Roster Ingestion Pipeline

A modular, three-stage pipeline for ingesting, validating, and managing healthcare provider roster data.

## Overview

This pipeline processes provider roster files from multiple sources, normalizes them into a consistent format, aggregates changes, and validates them against existing roster data. The system is designed to handle provider additions and terminations across multiple contracts.

## Architecture

The pipeline consists of three sequential stages:

```
┌─────────────────┐      ┌──────────────┐      ┌─────────────────┐
│  1. Normalize   │ ───> │ 2. Aggregate │ ───> │ 3. Check        │
│     Rosters     │      │   Rosters    │      │    Overlaps     │
└─────────────────┘      └──────────────┘      └─────────────────┘
       │                        │                      │
       ▼                        ▼                      ▼
  Individual CSVs      Consolidated CSV         Overlap Report
```

### Stage 1: Normalization (`stages/normalize.py`)

Processes raw roster files (Excel) into normalized CSV files.

**Inputs:**
- Raw Excel files from various providers
- Column mapping files (CSV)
- JSON configuration specifying file metadata

**Processing:**
- Applies column mappings to standardize field names
- Validates required fields (NPI, Tax ID, effective dates)
- Normalizes data formats (dates, tax IDs, names)
- Adds metadata (contract ID, tags, notes)
- Validates data quality

**Outputs:**
- One CSV file per input file in standardized format
- Saved to `data/{update_name}/processed/`

### Stage 2: Aggregation (`stages/aggregate.py`)

Combines all normalized files into a single change file.

**Inputs:**
- All normalized CSV files from Stage 1

**Processing:**
- Concatenates all normalized files
- Sorts by contract ID, action, and provider NPI
- Generates summary statistics

**Outputs:**
- Single consolidated CSV file: `{YYYYMMDD}_MarkCubanCompanies_AddTerms.csv`
- Saved to `output/`

### Stage 3: Overlap Checking (`stages/overlap_check.py`)

Validates aggregated changes against the existing roster.

**Inputs:**
- Aggregated change file from Stage 2
- Current full roster file (Excel or CSV)

**Processing:**
- Checks for NPI matches
- Checks for Tax ID matches
- Checks for NPI+Tax ID pair matches
- Checks for NPI+Tax ID+Contract ID matches
- Identifies potential issues (duplicate ADDs, missing TERMs)

**Outputs:**
- Overlap report CSV: `{YYYYMMDD}_overlap_check.csv`
- Saved to `output/`

## Quick Start

### Prerequisites

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install pandas openpyxl python-dateutil
```

### Basic Usage

```bash
# Run the full pipeline
python main.py -j 2025-10-oct.json

# Run with custom directories
python main.py -j 2025-10-oct.json -d data -o output

# Skip overlap checking
python main.py -j 2025-10-oct.json --skip-overlap

# Run only normalization
python main.py -j 2025-10-oct.json --normalize-only

# Specify custom roster file for overlap checking
python main.py -j 2025-10-oct.json -r output/20250923_MCC_FullRoster.xlsx
```

## Directory Structure

```
rockster/
├── main.py                      # Main entry point (orchestrator)
├── stages/                      # Pipeline stage modules
│   ├── __init__.py
│   ├── normalize.py            # Stage 1: Normalization
│   ├── aggregate.py            # Stage 2: Aggregation
│   └── overlap_check.py        # Stage 3: Overlap checking
├── utils.py                     # Utility functions
├── constants.py                 # Field definitions
├── data/                        # Data directory
│   ├── {update_name}/          # Update-specific directory
│   │   ├── raw/                # Raw Excel files
│   │   └── processed/          # Normalized CSV files
│   └── mapping_files/          # Column mapping files
├── output/                      # Output files
│   ├── {date}_AddTerms.csv     # Aggregated change file
│   └── {date}_overlap_check.csv # Overlap report
├── deprecated/                  # Legacy files (not used)
└── README.md                    # This file
```

## Configuration File Format

JSON configuration files specify the roster files to process:

```json
[
  {
    "filename": "Provider_Roster.xlsx",
    "sheet_name": "Sheet1",
    "mapping_file": "provider_mapping.csv",
    "contract_id": "C010",
    "action": "add",
    "note": "Monthly updates",
    "tag": "PROVIDER_TAG"
  }
]
```

### Configuration Fields

| Field | Required | Description |
|-------|----------|-------------|
| `filename` | Yes | Name of the Excel file in the raw directory |
| `sheet_name` | Yes | Sheet name to read from the Excel file |
| `mapping_file` | Yes | Name of the mapping file in mapping_files/ |
| `contract_id` | Yes | Contract identifier (e.g., C010, C001) |
| `action` | Yes | Action type: "add", "term", or "add & term" |
| `note` | No | Optional note to include in output |
| `tag` | Yes | Tag for grouping/categorization |

## Mapping Files

Mapping files are two-column CSV files (no header) that map source column names to standardized field names:

```csv
provider_npi,NPI
first_name,First Name
last_name,Last Name
tax_id,Tax ID
effective_date,Effective Date
...
```

**Format:**
- Column 1: Canonical field name (must match constants.py)
- Column 2: Source column name (as it appears in the Excel file)

## Required Fields

All roster files must include these standardized fields:

### Basic Fields
- `contract_id` - Contract identifier
- `action` - ADD or TERM
- `effective_date` - Date in YYYY-MM-DD format
- `provider_npi` - Provider NPI (required, cannot be empty)
- `tax_id` - Tax ID (required, cannot be empty)
- `first_name`, `last_name` - Provider name
- `degree`, `specialty_1` - Provider credentials
- `practice_name` - Practice/organization name

### Address Fields
- `address_line1`, `city`, `state`, `zip_code`, `phone`
- `address_line2` (optional)

### Billing Fields
- `billing_address_line1`, `billing_city`, `billing_state`, `billing_zip`
- `billing_npi` - Billing NPI
- `billing_address_line2` (optional)

### Metadata Fields
- `source_file` - Original filename (auto-populated)
- `note` - Notes/comments (optional)

## Command-Line Options

```
usage: main.py [-h] -j JSON [-d DATA_DIR] [-o OUTPUT_DIR] [-r ROSTER_FILE]
               [--skip-overlap] [--normalize-only] [--verbose]

Roster Ingestion Pipeline - Normalize, Aggregate, and Check Overlaps

required arguments:
  -j, --json JSON           Path to JSON configuration file

optional arguments:
  -h, --help                Show this help message and exit
  -d, --data-dir DATA_DIR   Base data directory (default: data)
  -o, --output-dir OUTPUT_DIR
                            Output directory (default: output)
  -r, --roster-file ROSTER_FILE
                            Path to existing full roster for overlap checking
                            (auto-detected if not provided)
  --skip-overlap            Skip the overlap checking stage
  --normalize-only          Run only the normalization stage
  --verbose                 Print verbose output
```

## Output Files

### Normalized Files (Stage 1)
- Location: `data/{update_name}/processed/`
- Format: `{contract_id}_{action}_{filename}.csv`
- Example: `C010_ADD_Provider_Roster.csv`

### Aggregated File (Stage 2)
- Location: `output/`
- Format: `{YYYYMMDD}_MarkCubanCompanies_AddTerms.csv`
- Contains: All normalized records combined and sorted

### Overlap Report (Stage 3)
- Location: `output/`
- Format: `{YYYYMMDD}_overlap_check.csv`
- Contains: All records with overlap flags:
  - `NPI_IN_ROSTER` - NPI exists in roster
  - `TIN_IN_ROSTER` - Tax ID exists in roster
  - `NPI_AND_TIN_IN_ROSTER` - NPI+Tax ID pair exists
  - `NPI_TIN_CID_MATCH` - Exact match (NPI+Tax ID+Contract ID)

## Workflow Example

1. **Prepare your data:**
   ```bash
   # Create update directory
   mkdir -p data/2025-10-oct/raw
   
   # Copy raw Excel files to raw directory
   cp *.xlsx data/2025-10-oct/raw/
   ```

2. **Create configuration file:**
   ```bash
   # Create JSON config specifying files and metadata
   nano 2025-10-oct.json
   ```

3. **Run the pipeline:**
   ```bash
   python main.py -j 2025-10-oct.json
   ```

4. **Review outputs:**
   ```bash
   # Check processed files
   ls data/2025-10-oct/processed/
   
   # Check aggregated file
   cat output/20251013_MarkCubanCompanies_AddTerms.csv
   
   # Review overlap report
   cat output/20251013_overlap_check.csv
   ```

## Validation Rules

### Normalization Stage
- ✓ All provider NPIs must be present (no empty values)
- ✓ All tax IDs must be present (no empty values)
- ✓ Effective dates must be in valid date format
- ✓ Actions must be ADD or TERM (or accepted variants)
- ⚠ First/last names should be present (warning only)

### Overlap Checking Stage
- ⚠ ADDs that already exist in roster (exact match)
- ⚠ TERMs that don't exist in roster (cannot terminate)

## Utility Scripts

### Other Scripts in Repository

- `compile_roster.py` - Applies change file to existing roster to create updated full roster
- `summary.py` - Generates summary statistics
- `example_usage.py` - Example usage patterns

## Troubleshooting

### Common Issues

**Error: "Data directory does not exist"**
- Ensure the directory structure matches the JSON config filename
- Example: `2025-10-oct.json` expects `data/2025-10-oct/`

**Error: "Empty provider NPIs found"**
- Check source Excel file for blank NPI rows
- Remove or populate empty rows in source file

**Error: "Effective date column not found"**
- Ensure mapping file includes `effective_date` mapping
- Verify source Excel has the date column

**Warning: "ADDs already in roster"**
- Review overlap report to identify duplicates
- May indicate re-additions or data quality issues

## Contributing

When adding new provider sources:

1. Create a mapping file in `data/mapping_files/`
2. Add configuration entry to your JSON file
3. Test with `--normalize-only` first
4. Review output and adjust mapping as needed

## License

Internal use only - Mark Cuban Companies Health Division

## Support

For questions or issues, contact the data team.

