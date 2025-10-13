"""
Stage 3: Overlap Checking

This module handles overlap checking against the existing roster.
It validates the aggregated change file against the current full roster to identify:
- NPIs already in roster
- Tax IDs already in roster
- NPI-TIN pairs already in roster
- NPI-TIN-Contract ID matches
"""

import os
import pandas as pd
from typing import Optional


def check_overlaps(
    change_file: str,
    roster_file: str,
    output_dir: str,
    output_filename: str = None
) -> str:
    """
    Check the aggregated change file for overlaps with the existing roster.
    
    Args:
        change_file: Path to the aggregated change file
        roster_file: Path to the existing full roster
        output_dir: Directory to save the overlap report
        output_filename: Optional custom filename for output
        
    Returns:
        Path to the overlap report file
    """
    print(f"\n{'#'*80}")
    print(f"STAGE 3: OVERLAP CHECKING")
    print(f"{'#'*80}")
    print(f"Change file: {change_file}")
    print(f"Roster file: {roster_file}")
    
    # Read the change file
    change_df = pd.read_csv(change_file, dtype=str)
    print(f"✓ Loaded change file: {len(change_df)} records")
    
    # Read the roster file
    if roster_file.endswith('.xlsx'):
        roster_df = pd.read_excel(roster_file, dtype=str)
    elif roster_file.endswith('.csv'):
        roster_df = pd.read_csv(roster_file, dtype=str)
    else:
        raise ValueError(f"Unsupported roster file type: {roster_file}")
    print(f"✓ Loaded roster file: {len(roster_df)} records")
    
    # Normalize data for comparison
    change_df = _normalize_for_comparison(change_df)
    roster_df = _normalize_for_comparison(roster_df)
    
    # Perform overlap checks
    overlap_df = _perform_overlap_checks(change_df, roster_df)
    
    # Print overlap summary
    _print_overlap_summary(overlap_df)
    
    # Generate output filename if not provided
    if output_filename is None:
        from datetime import datetime
        today = datetime.now().strftime('%Y%m%d')
        output_filename = f"{today}_overlap_check.csv"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save overlap report
    output_path = os.path.join(output_dir, output_filename)
    
    # Select columns for output
    output_cols = [
        'action', 'effective_date', 'contract_id',
        'provider_npi', 'first_name', 'middle_initial', 'last_name', 'degree',
        'specialty_1', 'tax_id', 'practice_name',
        'NPI_IN_ROSTER', 'TIN_IN_ROSTER', 'NPI_AND_TIN_IN_ROSTER', 'NPI_TIN_CID_MATCH',
        'note'
    ]
    
    # Only include columns that exist
    output_cols = [col for col in output_cols if col in overlap_df.columns]
    
    overlap_df[output_cols].to_csv(output_path, index=False)
    
    print(f"\n✓ Overlap report saved: {output_path}")
    
    return output_path


def _normalize_for_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize dataframe columns for comparison."""
    # Ensure key columns are strings and stripped
    for col in ['provider_npi', 'tax_id', 'contract_id']:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(lambda x: x.strip())
    
    # Ensure action is uppercase
    if 'action' in df.columns:
        df['action'] = df['action'].astype(str).apply(lambda x: x.upper().strip())
    
    return df


def _perform_overlap_checks(
    change_df: pd.DataFrame,
    roster_df: pd.DataFrame
) -> pd.DataFrame:
    """Perform overlap checks between change file and roster."""
    print(f"\nPerforming overlap checks...")
    
    # Filter to rows with valid NPIs and Tax IDs
    roster_npi_only = roster_df[roster_df['provider_npi'].notna()].copy()
    
    # Create lookup sets for efficient checking
    roster_npis = set(roster_npi_only['provider_npi'])
    roster_tins = set(roster_npi_only['tax_id'])
    roster_npi_tin_pairs = set(
        zip(roster_npi_only['provider_npi'], roster_npi_only['tax_id'])
    )
    roster_npi_tin_contract_triples = set(
        zip(
            roster_npi_only['provider_npi'],
            roster_npi_only['tax_id'],
            roster_npi_only['contract_id']
        )
    )
    
    # Perform checks
    change_df['NPI_IN_ROSTER'] = change_df['provider_npi'].isin(roster_npis)
    change_df['TIN_IN_ROSTER'] = change_df['tax_id'].isin(roster_tins)
    change_df['NPI_AND_TIN_IN_ROSTER'] = change_df.apply(
        lambda row: (row['provider_npi'], row['tax_id']) in roster_npi_tin_pairs,
        axis=1
    )
    change_df['NPI_TIN_CID_MATCH'] = change_df.apply(
        lambda row: (
            row['provider_npi'],
            row['tax_id'],
            row['contract_id']
        ) in roster_npi_tin_contract_triples,
        axis=1
    )
    
    print(f"✓ Overlap checks complete")
    
    return change_df


def _print_overlap_summary(df: pd.DataFrame):
    """Print summary statistics for overlap checks."""
    print(f"\n{'='*80}")
    print(f"OVERLAP CHECK SUMMARY")
    print(f"{'='*80}")
    
    total = len(df)
    
    # Overall statistics
    print(f"\nTotal Records Checked: {total}")
    
    npi_in_roster = df['NPI_IN_ROSTER'].sum()
    tin_in_roster = df['TIN_IN_ROSTER'].sum()
    npi_tin_in_roster = df['NPI_AND_TIN_IN_ROSTER'].sum()
    npi_tin_cid_match = df['NPI_TIN_CID_MATCH'].sum()
    
    print(f"\nOverlap Statistics:")
    print(f"  NPI in roster: {npi_in_roster} ({npi_in_roster/total*100:.1f}%)")
    print(f"  TIN in roster: {tin_in_roster} ({tin_in_roster/total*100:.1f}%)")
    print(f"  NPI+TIN in roster: {npi_tin_in_roster} ({npi_tin_in_roster/total*100:.1f}%)")
    print(f"  NPI+TIN+Contract ID match: {npi_tin_cid_match} ({npi_tin_cid_match/total*100:.1f}%)")
    
    # Breakdown by action
    if 'action' in df.columns:
        print(f"\nBreakdown by Action:")
        for action in sorted(df['action'].unique()):
            action_df = df[df['action'] == action]
            action_total = len(action_df)
            action_npi_tin_cid = action_df['NPI_TIN_CID_MATCH'].sum()
            print(f"  {action}: {action_total} total, {action_npi_tin_cid} matches")
    
    # Potential issues
    print(f"\nPotential Issues:")
    
    # ADDs that already exist
    adds = df[df['action'] == 'ADD']
    if len(adds) > 0:
        adds_exist = adds['NPI_TIN_CID_MATCH'].sum()
        print(f"  ADDs already in roster: {adds_exist}")
        if adds_exist > 0:
            print(f"    ⚠ Warning: {adds_exist} ADD records already exist in roster")
    
    # TERMs that don't exist
    terms = df[df['action'] == 'TERM']
    if len(terms) > 0:
        terms_not_exist = (~terms['NPI_TIN_CID_MATCH']).sum()
        print(f"  TERMs not in roster: {terms_not_exist}")
        if terms_not_exist > 0:
            print(f"    ⚠ Warning: {terms_not_exist} TERM records not found in roster")
    
    print(f"{'='*80}\n")

