"""
Stage 1: Normalization

This module handles the normalization of raw roster files.
It reads Excel files, applies mappings, validates data, and outputs standardized CSV files.
"""

import os
import pandas as pd
from typing import List, Dict
from utils import (
    read_roster_and_apply_mapping, concat_full_name, format_tax_id, 
    format_zip_code, format_phone, format_state, format_middle_initial,
    format_city, format_po_box
)
from constants import basic_fields, billing_fields, address_fields, metadata_fields


def normalize_roster_item(
    item: Dict,
    raw_dir: str,
    mapping_dir: str,
    processed_dir: str,
    accept_actions: List[str] = ['ADD', 'TERM']
) -> str:
    """
    Normalize a single raw roster file and save the processed dataframe.
    
    Args:
        item: Dictionary containing roster file metadata (filename, mapping, etc.)
        raw_dir: Directory containing raw roster files
        mapping_dir: Directory containing mapping files
        processed_dir: Directory to save processed files
        accept_actions: List of acceptable action values
        
    Returns:
        Path to the processed file
        
    Steps:
        1. Read the raw roster file and apply mapping
        2. Validate and normalize the 'action' column
        3. Validate and normalize the 'effective_date' column
        4. Add metadata columns (tag, note, contract_id, source_file)
        5. Validate required fields (NPIs, tax IDs, names)
        6. Create computed columns (full_name)
        7. Format data (tax_id, degree)
        8. Validate all required columns are present
        9. Save to CSV
    """
    print(f"\n{'='*80}")
    sheet_info = f" (Sheet: {item['sheet_name']})" if item.get('sheet_name') and item['sheet_name'] != 0 and item['sheet_name'] != '0' else ""
    print(f"Processing: {item['filename']}{sheet_info}")
    print(f"{'='*80}")
    
    # Construct file paths
    fpath = os.path.join(raw_dir, item['filename'])
    outfile = _generate_output_filename(item)
    outpath = os.path.join(processed_dir, outfile)
    
    # Read and apply mapping
    try:
        df = read_roster_and_apply_mapping(
            fpath=fpath,
            mapping_fpath=os.path.join(mapping_dir, item['mapping_file']),
            sheet_name=item['sheet_name']
        )
        print(f"✓ Read file successfully: {len(df)} rows")
    except Exception as e:
        print(f"✗ Error reading roster file: {item['filename']}")
        raise e
    
    # Validate and normalize action column
    df = _normalize_action_column(df, item, accept_actions)
    
    # Add metadata
    df['tag'] = item['tag']
    df['contract_id'] = item['contract_id']
    df['source_file'] = item['filename']
    
    # Validate and normalize note column
    df = _normalize_note_column(df, item)
    
    # Validate and normalize effective_date column
    df = _normalize_effective_date_column(df, item)
    
    # Validate required identifiers
    _validate_npis(df, item['filename'])
    _validate_tax_ids(df, item['filename'])
    _validate_names(df)
    
    # Create optional columns if missing
    optional_cols = ['address_line2', 'billing_address_line2', 'middle_initial', 'taxonomy_1']
    
    # For TERM actions, many additional fields are optional
    if item['action'] and 'term' in item['action'].lower():
        optional_cols.extend([
            # Basic fields that are optional for TERM
            'practice_name', 'degree', 'specialty_1',
            # Address fields
            'address_line1', 'city', 'state', 'zip_code', 'phone',
            # Billing fields
            'billing_address_line1', 'billing_city', 'billing_state', 
            'billing_zip', 'billing_npi'
        ])
    
    for col in optional_cols:
        if col not in df.columns:
            df[col] = ''
    
    # Create full name if not present
    if 'full_name' not in df.columns:
        df['full_name'] = df.apply(
            lambda x: concat_full_name(
                x['first_name'],
                x.get('middle_initial', ''),
                x['last_name']
            ),
            axis=1
        )
    
    # Format data
    df['tax_id'] = df['tax_id'].apply(format_tax_id)
    df['degree'] = df['degree'].apply(lambda x: str(x).replace('.', ''))
    
    # Format middle initial (single uppercase letter)
    df['middle_initial'] = df['middle_initial'].apply(format_middle_initial)
    
    # Format states (2-letter codes) - only if columns exist
    if 'state' in df.columns:
        df['state'] = df['state'].apply(format_state)
    if 'billing_state' in df.columns:
        df['billing_state'] = df['billing_state'].apply(format_state)
    
    # Format cities (title case) - only if columns exist
    if 'city' in df.columns:
        df['city'] = df['city'].apply(format_city)
    if 'billing_city' in df.columns:
        df['billing_city'] = df['billing_city'].apply(format_city)
    
    # Format zip codes (5-digit) - only if columns exist
    if 'zip_code' in df.columns:
        df['zip_code'] = df['zip_code'].apply(format_zip_code)
    if 'billing_zip' in df.columns:
        df['billing_zip'] = df['billing_zip'].apply(format_zip_code)
    
    # Format phone numbers - only if columns exist
    if 'phone' in df.columns:
        df['phone'] = df['phone'].apply(format_phone)
    if 'fax' in df.columns:
        df['fax'] = df['fax'].apply(format_phone)
    
    # Format PO Box in billing addresses - only if column exists
    if 'billing_address_line1' in df.columns:
        df['billing_address_line1'] = df['billing_address_line1'].apply(format_po_box)
    
    # Validate all required fields are present
    _validate_required_fields(df, item['filename'], item['action'])
    
    # Select and order columns - only include columns that exist
    all_fields = basic_fields + address_fields + billing_fields + metadata_fields
    available_fields = [col for col in all_fields if col in df.columns]
    processed_df = df[available_fields]
    
    # Save to CSV
    processed_df.to_csv(outpath, index=False)
    sheet_info = f" (Sheet: {item['sheet_name']})" if item.get('sheet_name') and item['sheet_name'] != 0 and item['sheet_name'] != '0' else ""
    print(f"✓ Saved processed file: {outfile}{sheet_info}")
    print(f"  Total rows: {len(processed_df)}")
    print(f"  Actions: {processed_df['action'].value_counts().to_dict()}")
    
    return outpath


def normalize_rosters(
    config: List[Dict],
    raw_dir: str,
    mapping_dir: str,
    processed_dir: str,
    roster_file: str = None
) -> List[str]:
    """
    Normalize all roster files specified in the configuration.
    
    Args:
        config: List of dictionaries containing roster file metadata
        raw_dir: Directory containing raw roster files
        mapping_dir: Directory containing mapping files
        processed_dir: Directory to save processed files
        roster_file: Optional path to existing roster for overlap checking
        
    Returns:
        List of paths to processed files
    """
    print(f"\n{'#'*80}")
    print(f"STAGE 1: NORMALIZATION")
    print(f"{'#'*80}")
    print(f"Processing {len(config)} roster files...")
    print(f"Raw directory: {raw_dir}")
    print(f"Processed directory: {processed_dir}")
    
    # Load roster for overlap checking if provided
    roster_df = None
    if roster_file:
        print(f"Loading roster for overlap checking: {roster_file}")
        if roster_file.endswith('.xlsx'):
            roster_df = pd.read_excel(roster_file, dtype=str)
        elif roster_file.endswith('.csv'):
            roster_df = pd.read_csv(roster_file, dtype=str)
        else:
            print(f"⚠ Warning: Unsupported roster file format: {roster_file}")
        
        if roster_df is not None:
            # Normalize roster data
            from .overlap_check import _normalize_for_comparison
            roster_df = _normalize_for_comparison(roster_df)
            print(f"✓ Loaded roster: {len(roster_df)} records")
    
    # Create processed directory if it doesn't exist
    os.makedirs(processed_dir, exist_ok=True)
    
    processed_files = []
    errors = []
    
    for i, item in enumerate(config, 1):
        sheet_info = f" (Sheet: {item['sheet_name']})" if item.get('sheet_name') and item['sheet_name'] != 0 and item['sheet_name'] != '0' else ""
        print(f"\n[{i}/{len(config)}]", end=" ")
        try:
            outpath = normalize_roster_item(
                item=item,
                raw_dir=raw_dir,
                mapping_dir=mapping_dir,
                processed_dir=processed_dir
            )
            
            # Add overlap checks if roster provided
            if roster_df is not None:
                _add_overlap_checks_to_file(outpath, roster_df)
            
            processed_files.append(outpath)
        except Exception as e:
            sheet_info = f" (Sheet: {item['sheet_name']})" if item.get('sheet_name') and item['sheet_name'] != 0 and item['sheet_name'] != '0' else ""
            error_msg = f"Failed to process {item['filename']}{sheet_info}: {str(e)}"
            print(f"✗ {error_msg}")
            errors.append(error_msg)
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"NORMALIZATION COMPLETE")
    print(f"{'='*80}")
    print(f"✓ Successfully processed: {len(processed_files)}/{len(config)}")
    if errors:
        print(f"✗ Failed: {len(errors)}")
        for error in errors:
            print(f"  - {error}")
    
    if errors and not processed_files:
        raise RuntimeError("All normalization tasks failed")
    
    return processed_files


def _generate_output_filename(item: Dict) -> str:
    """Generate a standardized output filename."""
    filename = item['filename'].replace('.xlsx', '.csv')
    filename = filename.replace(' ', '_').replace('-', '_').replace('/', '_')
    filename = filename.replace('(', '').replace(')', '')
    action_part = item['action'].upper().replace(' ', '').replace('&', 'AND')
    
    # Include sheet name if it's not the default sheet (0 or None)
    sheet_name = item.get('sheet_name')
    if sheet_name and sheet_name != 0 and sheet_name != '0':
        # Clean sheet name for filename
        clean_sheet = str(sheet_name).replace(' ', '_').replace('-', '_').replace('/', '_')
        clean_sheet = clean_sheet.replace('(', '').replace(')', '')
        return f"{item['contract_id']}_{action_part}_{clean_sheet}_{filename}"
    else:
        return f"{item['contract_id']}_{action_part}_{filename}"


def _normalize_action_column(
    df: pd.DataFrame,
    item: Dict,
    accept_actions: List[str]
) -> pd.DataFrame:
    """Validate and normalize the action column."""
    if 'action' not in df.columns:
        df['action'] = item['action'].upper()
        print(f"✓ Added action column from config: {item['action']}")
    else:
        print(f"✓ Action column exists in file")
        df['action'] = df['action'].apply(lambda x: str(x).upper().strip())
        
        # Validate actions
        invalid_actions = set(df['action'].unique()) - set(accept_actions)
        if invalid_actions:
            raise ValueError(
                f"Action column contains invalid values: {invalid_actions}. "
                f"Accepted values: {accept_actions}"
            )
    
    return df


def _normalize_note_column(df: pd.DataFrame, item: Dict) -> pd.DataFrame:
    """Validate and normalize the note column."""
    if 'note' not in df.columns:
        df['note'] = item.get('note', '')
    else:
        print(f"✓ Note column exists in file")
        df['note'] = df['note'].apply(lambda x: str(x).strip() if pd.notna(x) else '')
    
    return df


def _normalize_effective_date_column(df: pd.DataFrame, item: Dict) -> pd.DataFrame:
    """Validate and normalize the effective_date column."""
    if 'effective_date' not in df.columns:
        raise ValueError(
            f"Effective date column not found in file: {item['filename']}. "
            "This column is required."
        )
    
    print(f"✓ Effective date column exists")
    
    # Convert to YYYY-MM-DD format
    def to_yyyy_mm_dd(val):
        try:
            dt = pd.to_datetime(val, errors='coerce')
            if pd.isna(dt):
                return ""
            return dt.strftime('%Y-%m-%d')
        except Exception:
            return ""
    
    df['effective_date'] = df['effective_date'].apply(to_yyyy_mm_dd)
    
    # Print unique dates for verification
    unique_dates = df['effective_date'].unique()
    print(f"  Unique effective dates: {sorted([d for d in unique_dates if d])}")
    
    return df


def _validate_npis(df: pd.DataFrame, filename: str):
    """Validate that all provider NPIs are present."""
    if df['provider_npi'].isna().any() or (df['provider_npi'] == '').any():
        raise ValueError(f"Empty provider NPIs found in file: {filename}")
    print(f"✓ All provider NPIs are present ({df['provider_npi'].nunique()} unique)")


def _validate_tax_ids(df: pd.DataFrame, filename: str):
    """Validate that all tax IDs are present."""
    if df['tax_id'].isna().any() or (df['tax_id'] == '').any():
        raise ValueError(f"Empty tax IDs found in file: {filename}")
    print(f"✓ All tax IDs are present ({df['tax_id'].nunique()} unique)")


def _validate_names(df: pd.DataFrame):
    """Validate that names are present (with warnings for missing values)."""
    for col in ['first_name', 'last_name']:
        if df[col].isna().any() or (df[col] == '').any():
            count = (df[col].isna() | (df[col] == '')).sum()
            print(f"⚠ Warning: {count} rows have empty values in '{col}'")


def _validate_required_fields(df: pd.DataFrame, filename: str, action: str = None):
    """
    Validate that all required fields are present in the dataframe.
    For TERM actions, only core identifiers and metadata are required.
    """
    if action and 'term' in action.lower():
        # For TERM actions, only require core identifiers and metadata
        required_fields = [
            'contract_id', 'action', 'effective_date',
            'tax_id', 'provider_npi',
            'first_name', 'last_name',
            'tag', 'source_file', 'note'
        ]
        print(f"ℹ TERM action: only core identifiers and metadata required")
    else:
        # For ADD actions, all fields are required
        required_fields = basic_fields + address_fields + billing_fields + metadata_fields
    
    missing_fields = [col for col in required_fields if col not in df.columns]
    
    if missing_fields:
        raise ValueError(
            f"Missing required columns in file: {filename}\n"
            f"Missing: {missing_fields}"
        )
    
    print(f"✓ All required fields present ({len(required_fields)} columns)")


def _add_overlap_checks_to_file(filepath: str, roster_df: pd.DataFrame):
    """Add overlap check columns to a processed file."""
    from .overlap_check import _perform_overlap_checks, _normalize_for_comparison
    
    # Read the processed file
    df = pd.read_csv(filepath, dtype=str)
    df = _normalize_for_comparison(df)
    
    # Perform overlap checks
    df_with_checks = _perform_overlap_checks(df, roster_df)
    
    # Save back to file
    df_with_checks.to_csv(filepath, index=False)
    
    # Print summary
    matches = df_with_checks['NPI_TIN_CID_MATCH'].sum()
    print(f"  Overlap checks: {matches}/{len(df_with_checks)} matches in roster")

