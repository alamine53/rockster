""" process a single roster using only the fields 
required by TPA and pricing partner (no physical addresses)

set the filename, mapping file 
set the effective date and termination date 
map to contract ID and contracting entity
"""

import os
import pandas as pd
from datetime import datetime
from utils import load_mapping, format_tax_id, apply_mapping, _to_iso_date, build_full_name
import json
import argparse
import glob

# set the fields
basic_fields = ['contract_id', 'contracting_entity', 'action', 'effective_date', 'provider_npi','first_name', 'middle_initial', 'last_name', 'degree', 'specialty_1', 'tax_id', 'practice_name', 'note']
billing_fields = ['billing_address_line1', 'billing_address_line2', 'billing_city', 'billing_state', 'billing_zip', 'billing_npi']
address_fields = ['address_line1', 'address_line2', 'city', 'state', 'zip_code', 'phone']


def describe_df(df):
    # i want a table that describes for a set of columns, how many rows have empty values, 
    # how many rows have non-empty values, and how many unique values are in the column
    # print the table
    id_cols =['provider_npi', 'tax_id', 'billing_npi', 'first_name', 'last_name', 'action', 'effective_date']
    d = {col: {'n_rows': 0, 'unique_values': 0, 'empty_values': 0, 'non_empty_values': 0} for col in id_cols    }
    for col in id_cols:
        # empty values (count of "")
        empty_count = (df[col] == "").sum()
        d[col]['empty_values'] = empty_count
        # non-empty values (not "")
        non_empty_count = (df[col] != "").sum()
        d[col]['non_empty_values'] = non_empty_count
        # unique values (including "")
        unique_count = df[col].nunique()
        d[col]['unique_values'] = unique_count
        # total values
        total_count = df[col].count()
        d[col]['n_rows'] = total_count
    return d

def ingest_roster(fpath, sheet_name, mapping_fpath, contract_id, contracting_entity, outfile=None, verbose=False):
    
    # read the roster file and apply the mapping
    df = pd.read_excel(fpath, dtype=str, engine="openpyxl", sheet_name=sheet_name)
    mapping = load_mapping(mapping_fpath)
    df = apply_mapping(df, mapping)

    # make sure 'action' and 'effective_date' are in the dataframe
    if ('effective_date' not in df.columns or 'action' not in df.columns or
        df['effective_date'].isna().all() or (df['effective_date'] == '').all() or
        df['action'].isna().all() or (df['action'] == '').all()):
        # exit with error
        raise ValueError(f"ERROR: 'effective_date' or 'action' not in the dataframe")
        print(f"In order to ingest, the dataframe must have 'effective_date' and 'action' columns")
        print(df.columns)
        
    
    print('RAW FILE:\n', pd.DataFrame(describe_df(df)))
                
    # if any of these columsn have blank values, interrupt the code and fix
    # by either deleting the effective date or action
    for col in ['provider_npi']:
        if df[col].isna().any() or (df[col] == '').any():
            raise ValueError(f"ERROR: Some rows have empty values in required column '{col}'")
    
    for col in ['tax_id', 'first_name', 'last_name']:
        if df[col].isna().any() or (df[col] == '').any():
            print("WARNING: Some rows have empty values in required column '{col}'")
        
    # BEGIN INGESTION
    # drop rows where 'effective_date' or 'action' is empty
    df = df[(df['effective_date'] != '') & (df['action'] != '')]
    df['middle_initial'] = df['middle_initial'].apply(lambda x: x[0].upper() if x != '' else '')
    df['full_name'] = build_full_name(df)
    df['tax_id'] = df['tax_id'].apply(format_tax_id)
    df['degree'] = df['degree'].apply(lambda x: x.replace('.', ''))
    if 'practice_name' not in df.columns:
        df['practice_name'] = ''
    if 'note' not in df.columns:
        df['note'] = ''
    
    # check if any effective date is before 10/1
    # for index, row in df.iterrows():
    #     # if action is 'add' and effective date is before 10/1, change the effective date to 10/1
    #     if pd.to_datetime(row['effective_date']) < pd.to_datetime('2025-10-01'):
    #         print("Changing", row['effective_date'], "to 10/1")
    #         df.at[index, 'effective_date'] = '2025-10-01'


    # if billing NPI is missing, we're going to allow it
    if df['billing_npi'].isna().any() or (df['billing_npi'] == '').any():
        print("WARNING: Some rows have empty billing NPIs")

    # add empty columns for the fields that aren't always present
    for col in ['note', 'address_line2', 'provider_type']:
        if col not in df.columns:
            df[col] = ''

    # ensure all date fields are in ISO format
    df['effective_date'] = df['effective_date'].apply(_to_iso_date)
    df["contract_id"] = contract_id
    df["contracting_entity"] = contracting_entity
    df['source_file'] = fpath.split('/')[-1]
    df['source_sheet'] = sheet_name
    
    # add billing fields if they are not in the dataframe
    for col in billing_fields:
        if col not in df.columns:
            df[col] = ''
            
    # print the first 5 rows
    output_df = df[basic_fields + billing_fields + ["source_file"]]
    output_df = output_df.drop_duplicates(subset=['provider_npi', 'tax_id', 'billing_npi'])
    print('INGESTED FILE:\n', pd.DataFrame(describe_df(output_df)))

    if verbose:
        print(output_df.head())
        print(output_df.tail())
        print(output_df.columns)

    # save the output
    if outfile:
        print(f"Exporting to {outfile}")
        output_df.to_csv(outfile, index=False)
    
    return output_df

def get_files_for_date(date_str, output_dir='output'):
    """Get all CSV files for a specific date."""
    pattern = os.path.join(output_dir, f'{date_str}_*.csv')
    files = glob.glob(pattern)
    
    # Filter out any existing master files
    files = [f for f in files if 'MASTER_AddTerm' not in os.path.basename(f)]
    
    # Return just the filenames, not full paths
    return [os.path.basename(f) for f in files]

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('json_input', type=str)
    parser.add_argument('-d', '--data_dir', type=str, default='data')
    parser.add_argument('-o', '--output_dir', type=str, default='output')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-e', '--export', action='store_true')
    return parser.parse_args()

def main():
    # read the roster
    args = parse_args()
    print(args.json_input, args.output_dir)
    
    # create output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
        
    today = datetime.now().strftime('%Y%m%d')
    outdir = os.path.join(args.output_dir, today)
    if not os.path.exists(outdir) and args.export:
        os.makedirs(outdir)
    
    all_dfs = []
    with open(args.json_input, 'r') as f:
        i = json.load(f)    
    # make i into a list
    i = [i] if isinstance(i, dict) else i
    for item in i:
        # try:
        print(item)
        # read the roster and apply the mapping
        fpath = os.path.join(args.data_dir, 'roster_files', item['filename'])
        mapping_fpath = os.path.join(args.data_dir, 'mapping_files', item['mapping_file'])
        outfile = os.path.join(outdir, item['outfile']) if args.export else None
    
        all_dfs.append(ingest_roster(fpath, 
                    item['sheet_name'], 
                    mapping_fpath, 
                    item['contract_id'], 
                    item['contracting_entity'], 
                    outfile,
                    args.verbose)
        )
    
    df = pd.concat(all_dfs, ignore_index=True, sort=False)
    df.sort_values(['contract_id', 'action', 'provider_npi'], inplace=True)
    df.to_csv(os.path.join(args.output_dir, f'{today}_MarkCubanCompanies_SEP25_AddTerms.csv'), index=False)
if __name__ == "__main__":
    main()