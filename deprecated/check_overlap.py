""" take input change file and check NPIs against
existing full roster. 
- match by NPI only
- match by NPI and TIN
- match by NPI and TIN and Contract ID
"""

import pandas as pd
from datetime import datetime
import os
import argparse
from utils import load_mapping, apply_mapping, build_full_name

def read_change_file(change_file, sheet_name=None, mapping_file=None):
    """ read the change file """
    if change_file.endswith(".xlsx"):
        change_df = pd.read_excel(change_file, sheet_name=sheet_name)
        print(change_df.head())
        mapping = load_mapping(mapping_file)
        change_df = apply_mapping(change_df, mapping)
    elif change_file.endswith(".csv"):
        change_df = pd.read_csv(change_file)
    else:
        raise ValueError(f"Unsupported file type: {change_file}")
    change_df["action"] = change_df["action"].apply(lambda x: x.upper().strip())
    change_df["provider_npi"] = change_df["provider_npi"].astype(str).apply(lambda x: x.strip())
    return change_df

def read_roster_file(roster_file):
    """ read the roster file """
    roster_df = pd.read_excel(roster_file)
    roster_df["provider_npi"] = roster_df["provider_npi"].astype(str)
    return roster_df

def check_overlap(change_df, roster_df, outfile=None):
    """ check overlap between change file and roster file """

    # match by NPI only
    change_npi_only = change_df[change_df['provider_npi'].notna()]
    roster_npi_only = roster_df[roster_df['provider_npi'].notna()]

    # check if the (provider_npi, tax_id) pair exists in the roster
    roster_npi_tin_pairs = set(zip(roster_npi_only['provider_npi'], roster_npi_only['tax_id']))
    roster_npi_tin_contract_id_pairs = set(zip(roster_npi_only['provider_npi'], roster_npi_only['tax_id'], roster_npi_only['contract_id']))
    change_df["NPI_IN_ROSTER"] = change_df['provider_npi'].isin(roster_npi_only['provider_npi'])
    change_df["TIN_IN_ROSTER"] = change_df['tax_id'].isin(roster_npi_only['tax_id'])
    change_df["NPI_AND_TIN_IN_ROSTER"] = change_df.apply(
        lambda row: (row['provider_npi'], row['tax_id']) in roster_npi_tin_pairs, axis=1
    )
    change_df["NPI_TIN_CID_MATCH"] = change_df.apply(
        lambda row: (row['provider_npi'], row['tax_id'], row['contract_id']) in roster_npi_tin_contract_id_pairs, axis=1
    )
    return change_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file_path", type=str, required=True)
    parser.add_argument("-s", "--sheet_name", type=str, required=False)
    parser.add_argument("-m", "--mapping_file", type=str, required=False)
    parser.add_argument("-c", "--contract_id", type=str, required=False)
    parser.add_argument("-o", "--outfile", type=str, required=False)
    args = parser.parse_args()
    
    today = datetime.now().strftime('%Y%m%d')
    
    # read full roster file
    last_update_date = "20250923"
    latest_roster_file = os.path.join('output', f'{last_update_date}_MCC_FullRoster.xlsx')
    latest_roster_df = pd.read_excel(latest_roster_file)
    latest_roster_df["provider_npi"] = latest_roster_df["provider_npi"].astype(str)
    latest_roster_df["tax_id"] = latest_roster_df["tax_id"].astype(str)
    latest_roster_df["contract_id"] = latest_roster_df["contract_id"].astype(str)
    print(latest_roster_df.head())
    
    # read change file
    if args.file_path.endswith(".xlsx"):
        change_df = pd.read_excel(args.file_path)
    elif args.file_path.endswith(".csv"):
        change_df = pd.read_csv(args.file_path)
    else:
        raise ValueError(f"Unsupported file type: {args.change_file}")
    
    # apply mapping
    mapping = load_mapping(args.mapping_file) if args.mapping_file else None
    change_df = apply_mapping(change_df, mapping)
    change_df["provider_npi"] = change_df["provider_npi"].astype(str)
    change_df["tax_id"] = change_df["tax_id"].astype(str)
    if 'contract_id' not in change_df.columns:
        change_df["contract_id"] = args.contract_id
    change_df["contract_id"] = change_df["contract_id"].astype(str)

    outfile = args.outfile if args.outfile else f'{today}_check_overlap.csv'
    change_df = check_overlap(change_df, latest_roster_df, outfile)
    
    
    
     # export the resulting dataframe
    cols_to_keep = ['action', 'effective_date', 'provider_npi', 'first_name', 'middle_initial', 'last_name', 'degree', 'specialty_1', 'tax_id', 'NPI_IN_ROSTER', 'TIN_IN_ROSTER', 'NPI_AND_TIN_IN_ROSTER', 'NPI_TIN_CID_MATCH', 'note']
    if 'contract_id' in change_df.columns:
        cols_to_keep.insert(-1, 'contract_id')
        # cols_to_keep.insert(-1, 'contracting_entity')
        # cols_to_keep.insert(-1, 'source_file')
    change_df = change_df[cols_to_keep].sort_values(['provider_npi', 'tax_id'])
    if outfile:
        change_df.to_csv(outfile, index=False)
    print(change_df.head())
    
    # count stats by action and NPI_IN_ROSTER, TIN_IN_ROSTER, NPI_AND_TIN_IN_ROSTER
    print(change_df.groupby(['action', 'NPI_IN_ROSTER', 'TIN_IN_ROSTER', 'NPI_AND_TIN_IN_ROSTER', 'NPI_TIN_CID_MATCH']).size())