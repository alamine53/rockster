"""Unit tests for src/normalize.py"""

import pytest
import pandas as pd
import os
from src.normalize import (
    normalize_roster_item,
    normalize_rosters,
    _generate_output_filename,
    _normalize_action_column,
    _validate_npis,
    _validate_tax_ids
)


class TestGenerateOutputFilename:
    """Tests for _generate_output_filename function."""
    
    def test_basic_filename(self):
        """Test basic filename generation."""
        item = {
            'contract_id': 'C010',
            'action': 'add',
            'filename': 'Provider Roster.xlsx'
        }
        result = _generate_output_filename(item)
        assert result == 'C010_ADD_Provider_Roster.csv'
    
    def test_filename_with_special_chars(self):
        """Test filename with special characters."""
        item = {
            'contract_id': 'C010',
            'action': 'add & term',
            'filename': 'Provider-Roster (2025).xlsx'
        }
        result = _generate_output_filename(item)
        assert result == 'C010_ADDANDTERM_Provider_Roster_2025.csv'
    
    def test_filename_with_sheet_name(self):
        """Test filename with sheet name."""
        item = {
            'contract_id': 'C010',
            'action': 'add',
            'filename': 'Provider Roster.xlsx',
            'sheet_name': 'Sheet2'
        }
        result = _generate_output_filename(item)
        assert result == 'C010_ADD_Sheet2_Provider_Roster.csv'
    
    def test_filename_with_sheet_name_special_chars(self):
        """Test filename with sheet name containing special characters."""
        item = {
            'contract_id': 'C010',
            'action': 'add',
            'filename': 'Provider Roster.xlsx',
            'sheet_name': 'Sheet-Name (2025)'
        }
        result = _generate_output_filename(item)
        assert result == 'C010_ADD_Sheet_Name_2025_Provider_Roster.csv'
    
    def test_filename_with_default_sheet(self):
        """Test filename with default sheet (should not include sheet name)."""
        item = {
            'contract_id': 'C010',
            'action': 'add',
            'filename': 'Provider Roster.xlsx',
            'sheet_name': 0
        }
        result = _generate_output_filename(item)
        assert result == 'C010_ADD_Provider_Roster.csv'
    
    def test_filename_with_none_sheet(self):
        """Test filename with None sheet (should not include sheet name)."""
        item = {
            'contract_id': 'C010',
            'action': 'add',
            'filename': 'Provider Roster.xlsx',
            'sheet_name': None
        }
        result = _generate_output_filename(item)
        assert result == 'C010_ADD_Provider_Roster.csv'


class TestNormalizeActionColumn:
    """Tests for _normalize_action_column function."""
    
    def test_action_from_item(self):
        """Test adding action column from item config."""
        df = pd.DataFrame({'provider_npi': ['123']})
        item = {'action': 'add'}
        
        result = _normalize_action_column(df, item, ['ADD', 'TERM'])
        
        assert 'action' in result.columns
        assert result['action'].iloc[0] == 'ADD'
    
    def test_action_already_exists(self):
        """Test when action column already exists."""
        df = pd.DataFrame({'action': ['add', 'term'], 'provider_npi': ['123', '456']})
        item = {'action': 'add'}
        
        result = _normalize_action_column(df, item, ['ADD', 'TERM'])
        
        assert result['action'].iloc[0] == 'ADD'
        assert result['action'].iloc[1] == 'TERM'
    
    def test_invalid_action_raises_error(self):
        """Test that invalid actions raise an error."""
        df = pd.DataFrame({'action': ['invalid'], 'provider_npi': ['123']})
        item = {'action': 'add'}
        
        with pytest.raises(ValueError, match="invalid values"):
            _normalize_action_column(df, item, ['ADD', 'TERM'])


class TestValidateNPIs:
    """Tests for _validate_npis function."""
    
    def test_valid_npis(self):
        """Test validation passes with valid NPIs."""
        df = pd.DataFrame({'provider_npi': ['1234567890', '0987654321']})
        _validate_npis(df, 'test.xlsx')  # Should not raise
    
    def test_empty_npi_raises_error(self):
        """Test that empty NPIs raise an error."""
        df = pd.DataFrame({'provider_npi': ['1234567890', '']})
        
        with pytest.raises(ValueError, match="Empty provider NPIs"):
            _validate_npis(df, 'test.xlsx')
    
    def test_nan_npi_raises_error(self):
        """Test that NaN NPIs raise an error."""
        df = pd.DataFrame({'provider_npi': ['1234567890', None]})
        
        with pytest.raises(ValueError, match="Empty provider NPIs"):
            _validate_npis(df, 'test.xlsx')


class TestValidateTaxIds:
    """Tests for _validate_tax_ids function."""
    
    def test_valid_tax_ids(self):
        """Test validation passes with valid tax IDs."""
        df = pd.DataFrame({'tax_id': ['123456789', '987654321']})
        _validate_tax_ids(df, 'test.xlsx')  # Should not raise
    
    def test_empty_tax_id_raises_error(self):
        """Test that empty tax IDs raise an error."""
        df = pd.DataFrame({'tax_id': ['123456789', '']})
        
        with pytest.raises(ValueError, match="Empty tax IDs"):
            _validate_tax_ids(df, 'test.xlsx')


class TestNormalizeRosterItem:
    """Integration tests for normalize_roster_item function."""
    
    def test_normalize_basic_roster(self, temp_dir, create_test_excel, create_test_mapping, sample_config_item):
        """Test normalizing a basic roster file."""
        # Setup
        raw_dir = os.path.join(temp_dir, 'raw')
        mapping_dir = os.path.join(temp_dir, 'mapping')
        processed_dir = os.path.join(temp_dir, 'processed')
        os.makedirs(raw_dir)
        os.makedirs(mapping_dir)
        os.makedirs(processed_dir)
        
        # Create test files
        excel_path = create_test_excel(filename='test_roster.xlsx')
        os.rename(excel_path, os.path.join(raw_dir, 'test_roster.xlsx'))
        
        mapping_path = create_test_mapping(filename='test_mapping.csv')
        os.rename(mapping_path, os.path.join(mapping_dir, 'test_mapping.csv'))
        
        # Execute
        output_path = normalize_roster_item(
            item=sample_config_item,
            raw_dir=raw_dir,
            mapping_dir=mapping_dir,
            processed_dir=processed_dir
        )
        
        # Verify
        assert os.path.exists(output_path)
        result_df = pd.read_csv(output_path)
        assert len(result_df) == 3
        assert 'contract_id' in result_df.columns
        assert all(result_df['contract_id'] == 'C010')
        assert 'tag' in result_df.columns
        assert all(result_df['tag'] == 'TEST')
    
    def test_normalize_creates_full_name(self, temp_dir, create_test_excel, create_test_mapping, sample_config_item):
        """Test that full_name is created correctly."""
        # Setup
        raw_dir = os.path.join(temp_dir, 'raw')
        mapping_dir = os.path.join(temp_dir, 'mapping')
        processed_dir = os.path.join(temp_dir, 'processed')
        os.makedirs(raw_dir)
        os.makedirs(mapping_dir)
        os.makedirs(processed_dir)
        
        excel_path = create_test_excel()
        os.rename(excel_path, os.path.join(raw_dir, 'test_roster.xlsx'))
        
        mapping_path = create_test_mapping()
        os.rename(mapping_path, os.path.join(mapping_dir, 'test_mapping.csv'))
        
        # Execute
        output_path = normalize_roster_item(
            item=sample_config_item,
            raw_dir=raw_dir,
            mapping_dir=mapping_dir,
            processed_dir=processed_dir
        )
        
        # Verify
        result_df = pd.read_csv(output_path)
        assert 'full_name' in result_df.columns
        assert result_df['full_name'].iloc[0] == 'John A. Doe'


class TestNormalizeRosters:
    """Integration tests for normalize_rosters function."""
    
    def test_normalize_multiple_rosters(self, temp_dir, create_test_excel, create_test_mapping):
        """Test normalizing multiple roster files."""
        # Setup
        raw_dir = os.path.join(temp_dir, 'raw')
        mapping_dir = os.path.join(temp_dir, 'mapping')
        processed_dir = os.path.join(temp_dir, 'processed')
        os.makedirs(raw_dir)
        os.makedirs(mapping_dir)
        os.makedirs(processed_dir)
        
        # Create two test files
        for i in range(2):
            excel_path = create_test_excel(filename=f'roster_{i}.xlsx')
            os.rename(excel_path, os.path.join(raw_dir, f'roster_{i}.xlsx'))
        
        mapping_path = create_test_mapping()
        os.rename(mapping_path, os.path.join(mapping_dir, 'test_mapping.csv'))
        
        config = [
            {
                'filename': 'roster_0.xlsx',
                'sheet_name': 'Sheet1',
                'mapping_file': 'test_mapping.csv',
                'contract_id': 'C010',
                'action': 'add',
                'note': 'Test 0',
                'tag': 'TEST0'
            },
            {
                'filename': 'roster_1.xlsx',
                'sheet_name': 'Sheet1',
                'mapping_file': 'test_mapping.csv',
                'contract_id': 'C001',
                'action': 'term',
                'note': 'Test 1',
                'tag': 'TEST1'
            }
        ]
        
        # Execute
        processed_files = normalize_rosters(
            config=config,
            raw_dir=raw_dir,
            mapping_dir=mapping_dir,
            processed_dir=processed_dir
        )
        
        # Verify
        assert len(processed_files) == 2
        assert all(os.path.exists(f) for f in processed_files)

