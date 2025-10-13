"""
Main Entry Point for Roster Ingestion Pipeline

This script orchestrates the three-stage roster ingestion process:
1. Normalization - Standardize raw roster files
2. Aggregation - Combine normalized files into a single change file
3. Overlap Checking - Validate against existing roster

Usage:
    python main.py -j <json_config_file> [options]

Example:
    python main.py -j 2025-10-oct.json
    python main.py -j 2025-10-oct.json -d data -o output --skip-overlap
"""

import json
import argparse
import os
from datetime import datetime
from typing import Dict, List

from src import normalize_rosters, aggregate_rosters, check_overlaps


def parse_json(json_path: str) -> List[Dict]:
    """
    Parse the JSON configuration file.
    
    Args:
        json_path: Path to the JSON configuration file
        
    Returns:
        List of roster configuration dictionaries
    """
    try:
        with open(json_path, 'r') as f:
            config = json.load(f)
        
        # Ensure config is a list
        if isinstance(config, dict):
            config = [config]
        
        return config
    except FileNotFoundError:
        raise ValueError(f"JSON configuration file not found: {json_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error parsing JSON file: {e}")


def print_summary(config: List[Dict]):
    """Print summary statistics for the configuration."""
    print(f"\n{'='*80}")
    print(f"CONFIGURATION SUMMARY")
    print(f"{'='*80}")
    print(f"Total roster files: {len(config)}")
    
    # Count by action
    action_counts = {}
    for item in config:
        action = item['action']
        action_counts[action] = action_counts.get(action, 0) + 1
    
    print(f"\nAction breakdown:")
    for action, count in sorted(action_counts.items()):
        print(f"  {action}: {count}")
    
    # Count by contract
    contract_counts = {}
    for item in config:
        contract_id = item['contract_id']
        contract_counts[contract_id] = contract_counts.get(contract_id, 0) + 1
    
    print(f"\nContract breakdown:")
    for contract_id, count in sorted(contract_counts.items()):
        print(f"  {contract_id}: {count}")
    
    # Count by tag
    tag_counts = {}
    for item in config:
        tag = item.get('tag', 'N/A')
        tag_counts[tag] = tag_counts.get(tag, 0) + 1
    
    print(f"\nTag breakdown:")
    for tag, count in sorted(tag_counts.items()):
        print(f"  {tag}: {count}")
    
    print(f"{'='*80}\n")


def find_latest_roster(output_dir: str) -> str:
    """
    Find the latest full roster file in the output directory.
    
    Args:
        output_dir: Directory to search for roster files
        
    Returns:
        Path to the latest roster file
    """
    # Look for files matching the pattern *_MCC_FullRoster.xlsx
    import glob
    pattern = os.path.join(output_dir, '*_MCC_FullRoster.xlsx')
    roster_files = glob.glob(pattern)
    
    if not roster_files:
        raise FileNotFoundError(
            f"No full roster files found in {output_dir}. "
            "Looking for files matching pattern: *_MCC_FullRoster.xlsx"
        )
    
    # Sort by filename (assumes date prefix) and take the latest
    roster_files.sort(reverse=True)
    latest = roster_files[0]
    
    print(f"Found latest roster: {os.path.basename(latest)}")
    return latest


def main():
    """Main entry point for the roster ingestion pipeline."""
    parser = argparse.ArgumentParser(
        description='Roster Ingestion Pipeline - Normalize, Aggregate, and Check Overlaps',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Basic usage:
    python main.py -j 2025-10-oct.json
  
  Custom directories:
    python main.py -j 2025-10-oct.json -d data -o output
  
  Skip overlap checking:
    python main.py -j 2025-10-oct.json --skip-overlap
  
  Specify custom roster file:
    python main.py -j 2025-10-oct.json -r output/20250923_MCC_FullRoster.xlsx
        """
    )
    
    # Required arguments
    parser.add_argument(
        '-j', '--json',
        type=str,
        required=True,
        help='Path to JSON configuration file'
    )
    
    # Optional arguments
    parser.add_argument(
        '-d', '--data-dir',
        type=str,
        default='data',
        help='Base data directory (default: data)'
    )
    
    parser.add_argument(
        '-o', '--output-dir',
        type=str,
        default='output',
        help='Output directory (default: output)'
    )
    
    parser.add_argument(
        '-r', '--roster-file',
        type=str,
        default=None,
        help='Path to existing full roster for overlap checking (auto-detected if not provided)'
    )
    
    parser.add_argument(
        '--skip-overlap',
        action='store_true',
        help='Skip the overlap checking stage'
    )
    
    parser.add_argument(
        '--normalize-only',
        action='store_true',
        help='Run only the normalization stage'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print verbose output'
    )
    
    args = parser.parse_args()
    
    # Print header
    print(f"\n{'#'*80}")
    print(f"# ROSTER INGESTION PIPELINE")
    print(f"# {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*80}\n")
    
    # Parse configuration
    print(f"Loading configuration: {args.json}")
    config = parse_json(args.json)
    print_summary(config)
    
    # Determine directories
    update_name = os.path.splitext(os.path.basename(args.json))[0]
    data_dir = os.path.join(args.data_dir, update_name)
    mapping_dir = os.path.join(args.data_dir, 'mapping_files')
    raw_dir = os.path.join(data_dir, 'raw')
    
    # Output directory structure: output/update_name/
    output_dir = os.path.join(args.output_dir, update_name)
    processed_dir = os.path.join(output_dir, 'normalized')
    
    # Validate directories exist
    if not os.path.exists(data_dir):
        raise ValueError(f"Data directory does not exist: {data_dir}")
    if not os.path.exists(raw_dir):
        raise ValueError(f"Raw directory does not exist: {raw_dir}")
    if not os.path.exists(mapping_dir):
        raise ValueError(f"Mapping directory does not exist: {mapping_dir}")
    
    print(f"Directories:")
    print(f"  Data: {data_dir}")
    print(f"  Raw: {raw_dir}")
    print(f"  Mapping: {mapping_dir}")
    print(f"  Output: {output_dir}")
    print(f"  Normalized: {processed_dir}")
    
    # Find or use specified roster file for overlap checking
    roster_file = None
    if not args.skip_overlap:
        if args.roster_file:
            roster_file = args.roster_file
            if not os.path.exists(roster_file):
                raise FileNotFoundError(f"Specified roster file not found: {roster_file}")
            print(f"  Roster: {roster_file}")
        else:
            try:
                roster_file = find_latest_roster(args.output_dir)
                print(f"  Roster: {roster_file}")
            except FileNotFoundError as e:
                print(f"\n⚠ Warning: {e}")
                print(f"  Continuing without overlap checking.\n")
                roster_file = None
    
    # STAGE 1: Normalization (with overlap checks if roster available)
    processed_files = normalize_rosters(
        config=config,
        raw_dir=raw_dir,
        mapping_dir=mapping_dir,
        processed_dir=processed_dir,
        roster_file=roster_file
    )
    
    if args.normalize_only:
        print(f"\n{'#'*80}")
        print(f"# PIPELINE COMPLETE (NORMALIZATION ONLY)")
        print(f"{'#'*80}\n")
        return
    
    # STAGE 2: Aggregation (preserves overlap columns if they exist)
    today = datetime.now().strftime('%Y%m%d')
    aggregated_filename = f"{today}_{update_name}_aggregated.csv"
    aggregated_file = aggregate_rosters(
        processed_files=processed_files,
        output_dir=output_dir,
        output_filename=aggregated_filename
    )
    
    # Generate summary report
    _generate_summary_report(
        aggregated_file=aggregated_file,
        output_dir=output_dir,
        update_name=update_name,
        roster_file=roster_file
    )
    
    # Final summary
    print(f"\n{'#'*80}")
    print(f"# PIPELINE COMPLETE")
    print(f"{'#'*80}")
    print(f"\nAll outputs saved to: {output_dir}/")
    print(f"  Normalized files: normalized/ ({len(processed_files)} files)")
    print(f"  Aggregated file: {os.path.basename(aggregated_file)}")
    print(f"  Summary report: summary.txt")
    print(f"\n{'#'*80}\n")


def _generate_summary_report(
    aggregated_file: str,
    output_dir: str,
    update_name: str,
    roster_file: str = None
):
    """Generate a summary report of the pipeline run."""
    import pandas as pd
    from datetime import datetime
    
    print(f"\n{'#'*80}")
    print(f"GENERATING SUMMARY REPORT")
    print(f"{'#'*80}")
    
    # Read aggregated file
    df = pd.read_csv(aggregated_file, dtype=str)
    
    summary_lines = []
    summary_lines.append("="*80)
    summary_lines.append(f"ROSTER UPDATE SUMMARY: {update_name}")
    summary_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    summary_lines.append("="*80)
    summary_lines.append("")
    
    # Basic stats
    summary_lines.append(f"Total Records: {len(df)}")
    summary_lines.append(f"Unique Providers (NPI): {df['provider_npi'].nunique()}")
    if 'tax_id' in df.columns:
        summary_lines.append(f"Unique Tax IDs: {df['tax_id'].nunique()}")
    if 'contract_id' in df.columns:
        summary_lines.append(f"Unique Contracts: {df['contract_id'].nunique()}")
    summary_lines.append("")
    
    # Action breakdown
    if 'action' in df.columns:
        summary_lines.append("ACTION BREAKDOWN:")
        action_counts = df['action'].value_counts().sort_index()
        for action, count in action_counts.items():
            pct = count / len(df) * 100
            summary_lines.append(f"  {action}: {count} ({pct:.1f}%)")
        summary_lines.append("")
    
    # Contract breakdown
    if 'contract_id' in df.columns:
        summary_lines.append("CONTRACT BREAKDOWN:")
        contract_counts = df['contract_id'].value_counts().sort_index()
        for contract_id, count in contract_counts.items():
            summary_lines.append(f"  {contract_id}: {count}")
        summary_lines.append("")
    
    # Tag breakdown
    if 'tag' in df.columns:
        summary_lines.append("TAG BREAKDOWN:")
        tag_counts = df['tag'].value_counts().sort_index()
        for tag, count in tag_counts.items():
            summary_lines.append(f"  {tag}: {count}")
        summary_lines.append("")
    
    # Overlap statistics (if available)
    if 'NPI_TIN_CID_MATCH' in df.columns:
        summary_lines.append("OVERLAP ANALYSIS:")
        summary_lines.append(f"  Roster file: {os.path.basename(roster_file) if roster_file else 'N/A'}")
        summary_lines.append("")
        
        total = len(df)
        # Convert to bool for summing
        npi_match = int((df['NPI_IN_ROSTER'] == 'True').sum()) if 'NPI_IN_ROSTER' in df.columns else 0
        tin_match = int((df['TIN_IN_ROSTER'] == 'True').sum()) if 'TIN_IN_ROSTER' in df.columns else 0
        npi_tin_match = int((df['NPI_AND_TIN_IN_ROSTER'] == 'True').sum()) if 'NPI_AND_TIN_IN_ROSTER' in df.columns else 0
        cid_match = int((df['NPI_TIN_CID_MATCH'] == 'True').sum())
        
        summary_lines.append(f"  NPI in roster: {npi_match} ({npi_match/total*100:.1f}%)")
        summary_lines.append(f"  TIN in roster: {tin_match} ({tin_match/total*100:.1f}%)")
        summary_lines.append(f"  NPI+TIN in roster: {npi_tin_match} ({npi_tin_match/total*100:.1f}%)")
        summary_lines.append(f"  NPI+TIN+CID match: {cid_match} ({cid_match/total*100:.1f}%)")
        summary_lines.append("")
        
        # Action-specific overlap
        summary_lines.append("  By Action:")
        for action in sorted(df['action'].unique()):
            action_df = df[df['action'] == action]
            action_matches = int((action_df['NPI_TIN_CID_MATCH'] == 'True').sum())
            summary_lines.append(f"    {action}: {len(action_df)} total, {action_matches} matches")
        summary_lines.append("")
        
        # Potential issues
        adds = df[df['action'] == 'ADD']
        terms = df[df['action'] == 'TERM']
        
        if len(adds) > 0:
            adds_exist = int((adds['NPI_TIN_CID_MATCH'] == 'True').sum())
            if adds_exist > 0:
                summary_lines.append(f"  ⚠ WARNING: {adds_exist} ADD records already exist in roster")
        
        if len(terms) > 0:
            terms_not_exist = int((terms['NPI_TIN_CID_MATCH'] != 'True').sum())
            if terms_not_exist > 0:
                summary_lines.append(f"  ⚠ WARNING: {terms_not_exist} TERM records not found in roster")
        summary_lines.append("")
    
    # Effective date range
    if 'effective_date' in df.columns:
        try:
            dates = pd.to_datetime(df['effective_date'], errors='coerce')
            valid_dates = dates.dropna()
            if len(valid_dates) > 0:
                summary_lines.append("EFFECTIVE DATE RANGE:")
                summary_lines.append(f"  Earliest: {valid_dates.min().strftime('%Y-%m-%d')}")
                summary_lines.append(f"  Latest: {valid_dates.max().strftime('%Y-%m-%d')}")
                summary_lines.append("")
        except:
            pass
    
    summary_lines.append("="*80)
    
    # Write to file
    summary_path = os.path.join(output_dir, 'summary.txt')
    with open(summary_path, 'w') as f:
        f.write('\n'.join(summary_lines))
    
    # Print to console
    for line in summary_lines:
        print(line)
    
    print(f"\n✓ Summary saved to: {summary_path}")


if __name__ == "__main__":
    main()




