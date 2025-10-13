"""
Stage 2: Aggregation

This module handles the aggregation of normalized roster files.
It combines all normalized CSV files into a single consolidated change file.
"""

import os
import pandas as pd
from typing import List, Dict
from datetime import datetime


def aggregate_rosters(
    processed_files: List[str],
    output_dir: str,
    output_filename: str = None
) -> str:
    """
    Aggregate all processed roster files into a single change file.
    
    Args:
        processed_files: List of paths to processed CSV files
        output_dir: Directory to save the aggregated file
        output_filename: Optional custom filename for output (auto-generated if None)
        
    Returns:
        Path to the aggregated file
    """
    print(f"\n{'#'*80}")
    print(f"STAGE 2: AGGREGATION")
    print(f"{'#'*80}")
    print(f"Aggregating {len(processed_files)} processed files...")
    
    if not processed_files:
        raise ValueError("No processed files to aggregate")
    
    # Read all processed files
    dfs = []
    for fpath in processed_files:
        try:
            df = pd.read_csv(fpath, dtype=str)
            dfs.append(df)
            print(f"✓ Loaded: {os.path.basename(fpath)} ({len(df)} rows)")
        except Exception as e:
            print(f"✗ Failed to load: {fpath}")
            raise e
    
    # Concatenate all dataframes
    aggregated_df = pd.concat(dfs, ignore_index=True, sort=False)
    
    # Sort for consistency
    aggregated_df.sort_values(
        by=['contract_id', 'action', 'provider_npi'],
        inplace=True
    )
    
    # Generate summary statistics
    _print_aggregation_summary(aggregated_df)
    
    # Generate output filename if not provided
    if output_filename is None:
        today = datetime.now().strftime('%Y%m%d')
        output_filename = f"{today}_MarkCubanCompanies_AddTerms.csv"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save aggregated file
    output_path = os.path.join(output_dir, output_filename)
    aggregated_df.to_csv(output_path, index=False)
    
    print(f"\n✓ Aggregated file saved: {output_path}")
    print(f"  Total rows: {len(aggregated_df)}")
    print(f"  Total columns: {len(aggregated_df.columns)}")
    
    return output_path


def _print_aggregation_summary(df: pd.DataFrame):
    """Print summary statistics for the aggregated data."""
    print(f"\n{'='*80}")
    print(f"AGGREGATION SUMMARY")
    print(f"{'='*80}")
    
    # Total counts
    print(f"\nTotal Records: {len(df)}")
    if 'provider_npi' in df.columns:
        print(f"Unique Providers (NPI): {df['provider_npi'].nunique()}")
    if 'tax_id' in df.columns:
        print(f"Unique Tax IDs: {df['tax_id'].nunique()}")
    if 'contract_id' in df.columns:
        print(f"Unique Contracts: {df['contract_id'].nunique()}")
    
    # Action breakdown
    print(f"\nAction Breakdown:")
    action_counts = df['action'].value_counts().sort_index()
    for action, count in action_counts.items():
        print(f"  {action}: {count}")
    
    # Contract breakdown
    print(f"\nContract Breakdown:")
    contract_counts = df['contract_id'].value_counts().sort_index()
    for contract_id, count in contract_counts.items():
        print(f"  {contract_id}: {count}")
    
    # Tag breakdown
    if 'tag' in df.columns:
        print(f"\nTag Breakdown:")
        tag_counts = df['tag'].value_counts().sort_index()
        for tag, count in tag_counts.items():
            print(f"  {tag}: {count}")
    
    # Effective date range
    if 'effective_date' in df.columns:
        try:
            dates = pd.to_datetime(df['effective_date'], errors='coerce')
            valid_dates = dates.dropna()
            if len(valid_dates) > 0:
                print(f"\nEffective Date Range:")
                print(f"  Earliest: {valid_dates.min().strftime('%Y-%m-%d')}")
                print(f"  Latest: {valid_dates.max().strftime('%Y-%m-%d')}")
        except:
            pass
    
    print(f"{'='*80}\n")

