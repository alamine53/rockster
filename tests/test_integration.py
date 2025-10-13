"""Integration tests for the full pipeline."""

import pytest
import pandas as pd
import os
import json
from src import normalize_rosters, aggregate_rosters, check_overlaps


class TestFullPipeline:
    """End-to-end integration tests."""
    
    def test_full_pipeline_execution(self, temp_dir, create_test_excel, create_test_mapping, sample_roster_data):
        """Test the complete pipeline from normalization to overlap checking."""
        # Setup directory structure
        raw_dir = os.path.join(temp_dir, 'raw')
        mapping_dir = os.path.join(temp_dir, 'mapping')
        processed_dir = os.path.join(temp_dir, 'processed')
        output_dir = os.path.join(temp_dir, 'output')
        
        for d in [raw_dir, mapping_dir, processed_dir, output_dir]:
            os.makedirs(d)
        
        # Create test data files
        excel1 = create_test_excel(filename='roster1.xlsx')
        os.rename(excel1, os.path.join(raw_dir, 'roster1.xlsx'))
        
        excel2 = create_test_excel(filename='roster2.xlsx')
        os.rename(excel2, os.path.join(raw_dir, 'roster2.xlsx'))
        
        mapping = create_test_mapping()
        os.rename(mapping, os.path.join(mapping_dir, 'test_mapping.csv'))
        
        # Create roster file
        roster_path = os.path.join(output_dir, 'existing_roster.xlsx')
        sample_roster_data.to_excel(roster_path, index=False)
        
        # Configuration
        config = [
            {
                'filename': 'roster1.xlsx',
                'sheet_name': 'Sheet1',
                'mapping_file': 'test_mapping.csv',
                'contract_id': 'C010',
                'action': 'add',
                'note': 'Test 1',
                'tag': 'TEST1'
            },
            {
                'filename': 'roster2.xlsx',
                'sheet_name': 'Sheet1',
                'mapping_file': 'test_mapping.csv',
                'contract_id': 'C001',
                'action': 'term',
                'note': 'Test 2',
                'tag': 'TEST2'
            }
        ]
        
        # Stage 1: Normalize
        processed_files = normalize_rosters(
            config=config,
            raw_dir=raw_dir,
            mapping_dir=mapping_dir,
            processed_dir=processed_dir
        )
        
        assert len(processed_files) == 2
        assert all(os.path.exists(f) for f in processed_files)
        
        # Stage 2: Aggregate
        aggregated_file = aggregate_rosters(
            processed_files=processed_files,
            output_dir=output_dir,
            output_filename='aggregated.csv'
        )
        
        assert os.path.exists(aggregated_file)
        agg_df = pd.read_csv(aggregated_file)
        assert len(agg_df) == 6  # 3 rows * 2 files
        
        # Stage 3: Check overlaps
        overlap_file = check_overlaps(
            change_file=aggregated_file,
            roster_file=roster_path,
            output_dir=output_dir,
            output_filename='overlap.csv'
        )
        
        assert os.path.exists(overlap_file)
        overlap_df = pd.read_csv(overlap_file)
        assert 'NPI_IN_ROSTER' in overlap_df.columns
        assert 'NPI_TIN_CID_MATCH' in overlap_df.columns
    
    def test_pipeline_with_errors_in_normalization(self, temp_dir, create_test_mapping):
        """Test pipeline behavior when normalization encounters errors."""
        raw_dir = os.path.join(temp_dir, 'raw')
        mapping_dir = os.path.join(temp_dir, 'mapping')
        processed_dir = os.path.join(temp_dir, 'processed')
        
        for d in [raw_dir, mapping_dir, processed_dir]:
            os.makedirs(d)
        
        # Create a valid Excel file
        valid_df = pd.DataFrame({
            'NPI': ['1234567890'],
            'First Name': ['John'],
            'Last Name': ['Doe'],
            'Tax ID': ['123456789'],
            'Effective Date': ['2025-10-01'],
            'Action': ['ADD']
        })
        valid_path = os.path.join(raw_dir, 'valid.xlsx')
        valid_df.to_excel(valid_path, index=False)
        
        # Create mapping
        mapping = create_test_mapping()
        os.rename(mapping, os.path.join(mapping_dir, 'test_mapping.csv'))
        
        config = [
            {
                'filename': 'valid.xlsx',
                'sheet_name': 'Sheet1',
                'mapping_file': 'test_mapping.csv',
                'contract_id': 'C010',
                'action': 'add',
                'note': '',
                'tag': 'TEST'
            },
            {
                'filename': 'nonexistent.xlsx',  # This will fail
                'sheet_name': 'Sheet1',
                'mapping_file': 'test_mapping.csv',
                'contract_id': 'C001',
                'action': 'add',
                'note': '',
                'tag': 'TEST'
            }
        ]
        
        # The function should handle errors gracefully
        # It processes valid files and reports errors for invalid ones
        processed_files = normalize_rosters(
            config=config,
            raw_dir=raw_dir,
            mapping_dir=mapping_dir,
            processed_dir=processed_dir
        )
        
        # Should have processed the valid file only
        assert len(processed_files) == 1


class TestDataQuality:
    """Tests for data quality checks throughout the pipeline."""
    
    def test_duplicate_npis_handled(self, temp_dir, create_test_mapping):
        """Test handling of duplicate NPIs in input data."""
        raw_dir = os.path.join(temp_dir, 'raw')
        mapping_dir = os.path.join(temp_dir, 'mapping')
        processed_dir = os.path.join(temp_dir, 'processed')
        
        for d in [raw_dir, mapping_dir, processed_dir]:
            os.makedirs(d)
        
        # Create data with duplicate NPIs
        df = pd.DataFrame({
            'NPI': ['1234567890', '1234567890', '0987654321'],  # Duplicate
            'First Name': ['John', 'John', 'Jane'],
            'Last Name': ['Doe', 'Doe', 'Smith'],
            'Tax ID': ['123456789', '123456789', '987654321'],
            'Effective Date': ['2025-10-01', '2025-10-01', '2025-10-15'],
            'Action': ['ADD', 'ADD', 'ADD'],
            'Middle Initial': ['A', 'A', 'B'],
            'Degree': ['MD', 'MD', 'DO'],
            'Specialty': ['Cardiology', 'Cardiology', 'Pediatrics'],
            'Practice Name': ['Heart Care', 'Heart Care', 'Kids Health'],
            'Address Line 1': ['123 Main', '123 Main', '456 Oak'],
            'City': ['Dallas', 'Dallas', 'Austin'],
            'State': ['TX', 'TX', 'TX'],
            'Zip': ['75001', '75001', '78701'],
            'Phone': ['214-555-0001', '214-555-0001', '512-555-0002'],
            'Billing Address Line 1': ['123 Main', '123 Main', '456 Oak'],
            'Billing City': ['Dallas', 'Dallas', 'Austin'],
            'Billing State': ['TX', 'TX', 'TX'],
            'Billing Zip': ['75001', '75001', '78701'],
            'Billing NPI': ['1234567890', '1234567890', '0987654321']
        })
        
        excel_path = os.path.join(raw_dir, 'duplicates.xlsx')
        df.to_excel(excel_path, index=False)
        
        mapping = create_test_mapping()
        os.rename(mapping, os.path.join(mapping_dir, 'test_mapping.csv'))
        
        config = [{
            'filename': 'duplicates.xlsx',
            'sheet_name': 'Sheet1',
            'mapping_file': 'test_mapping.csv',
            'contract_id': 'C010',
            'action': 'add',
            'note': '',
            'tag': 'TEST'
        }]
        
        # Execute - should process without errors
        processed_files = normalize_rosters(
            config=config,
            raw_dir=raw_dir,
            mapping_dir=mapping_dir,
            processed_dir=processed_dir
        )
        
        # Verify duplicates are preserved (not deduplicated in normalization)
        result_df = pd.read_csv(processed_files[0])
        assert len(result_df) == 3  # All rows preserved

