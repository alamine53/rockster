"""Unit tests for src/overlap_check.py"""

import pytest
import pandas as pd
import os
from src.overlap_check import (
    check_overlaps,
    _normalize_for_comparison,
    _perform_overlap_checks
)


class TestNormalizeForComparison:
    """Tests for _normalize_for_comparison function."""
    
    def test_normalize_strips_whitespace(self):
        """Test that whitespace is stripped from key columns."""
        df = pd.DataFrame({
            'provider_npi': [' 1234567890 ', '0987654321'],
            'tax_id': ['123456789 ', ' 987654321'],
            'contract_id': [' C010', 'C001 ']
        })
        
        result = _normalize_for_comparison(df)
        
        assert result['provider_npi'].iloc[0] == '1234567890'
        assert result['tax_id'].iloc[0] == '123456789'
        assert result['contract_id'].iloc[0] == 'C010'
    
    def test_normalize_uppercases_action(self):
        """Test that action is uppercased."""
        df = pd.DataFrame({
            'action': ['add', 'term', 'ADD'],
            'provider_npi': ['123', '456', '789']
        })
        
        result = _normalize_for_comparison(df)
        
        assert all(result['action'] == 'ADD') or result['action'].iloc[1] == 'TERM'
        assert result['action'].iloc[0] == 'ADD'
        assert result['action'].iloc[1] == 'TERM'


class TestPerformOverlapChecks:
    """Tests for _perform_overlap_checks function."""
    
    def test_npi_in_roster_check(self):
        """Test NPI_IN_ROSTER flag."""
        change_df = pd.DataFrame({
            'provider_npi': ['1234567890', '9999999999'],
            'tax_id': ['123456789', '999999999'],
            'contract_id': ['C010', 'C010']
        })
        
        roster_df = pd.DataFrame({
            'provider_npi': ['1234567890', '5555555555'],
            'tax_id': ['123456789', '555555555'],
            'contract_id': ['C010', 'C001']
        })
        
        result = _perform_overlap_checks(change_df, roster_df)
        
        assert result['NPI_IN_ROSTER'].iloc[0] == True
        assert result['NPI_IN_ROSTER'].iloc[1] == False
    
    def test_npi_tin_match(self):
        """Test NPI_AND_TIN_IN_ROSTER flag."""
        change_df = pd.DataFrame({
            'provider_npi': ['1234567890', '1234567890'],
            'tax_id': ['123456789', '999999999'],
            'contract_id': ['C010', 'C010']
        })
        
        roster_df = pd.DataFrame({
            'provider_npi': ['1234567890'],
            'tax_id': ['123456789'],
            'contract_id': ['C010']
        })
        
        result = _perform_overlap_checks(change_df, roster_df)
        
        assert result['NPI_AND_TIN_IN_ROSTER'].iloc[0] == True
        assert result['NPI_AND_TIN_IN_ROSTER'].iloc[1] == False
    
    def test_npi_tin_cid_match(self):
        """Test NPI_TIN_CID_MATCH flag."""
        change_df = pd.DataFrame({
            'provider_npi': ['1234567890', '1234567890'],
            'tax_id': ['123456789', '123456789'],
            'contract_id': ['C010', 'C001']
        })
        
        roster_df = pd.DataFrame({
            'provider_npi': ['1234567890'],
            'tax_id': ['123456789'],
            'contract_id': ['C010']
        })
        
        result = _perform_overlap_checks(change_df, roster_df)
        
        assert result['NPI_TIN_CID_MATCH'].iloc[0] == True
        assert result['NPI_TIN_CID_MATCH'].iloc[1] == False


class TestCheckOverlaps:
    """Integration tests for check_overlaps function."""
    
    def test_check_overlaps_basic(self, temp_dir):
        """Test basic overlap checking."""
        # Create change file
        change_df = pd.DataFrame({
            'contract_id': ['C010', 'C010'],
            'action': ['ADD', 'TERM'],
            'provider_npi': ['1234567890', '0987654321'],
            'tax_id': ['123456789', '987654321'],
            'first_name': ['John', 'Jane'],
            'last_name': ['Doe', 'Smith'],
            'middle_initial': ['A', 'B'],
            'degree': ['MD', 'DO'],
            'specialty_1': ['Cardiology', 'Pediatrics'],
            'practice_name': ['Heart Care', 'Kids Health'],
            'effective_date': ['2025-10-01', '2025-10-15'],
            'note': ['', '']
        })
        
        change_path = os.path.join(temp_dir, 'change.csv')
        change_df.to_csv(change_path, index=False)
        
        # Create roster file
        roster_df = pd.DataFrame({
            'provider_npi': ['1234567890'],
            'tax_id': ['123456789'],
            'contract_id': ['C010']
        })
        
        roster_path = os.path.join(temp_dir, 'roster.csv')
        roster_df.to_csv(roster_path, index=False)
        
        # Execute
        output_path = check_overlaps(
            change_file=change_path,
            roster_file=roster_path,
            output_dir=temp_dir,
            output_filename='overlap.csv'
        )
        
        # Verify
        assert os.path.exists(output_path)
        result_df = pd.read_csv(output_path)
        assert 'NPI_IN_ROSTER' in result_df.columns
        assert 'NPI_TIN_CID_MATCH' in result_df.columns
        assert result_df['NPI_IN_ROSTER'].iloc[0] == True
        assert result_df['NPI_IN_ROSTER'].iloc[1] == False
    
    def test_check_overlaps_with_excel_roster(self, temp_dir):
        """Test overlap checking with Excel roster file."""
        # Create change file
        change_df = pd.DataFrame({
            'contract_id': ['C010'],
            'action': ['ADD'],
            'provider_npi': ['1234567890'],
            'tax_id': ['123456789'],
            'first_name': ['John'],
            'last_name': ['Doe'],
            'middle_initial': ['A'],
            'degree': ['MD'],
            'specialty_1': ['Cardiology'],
            'practice_name': ['Heart Care'],
            'effective_date': ['2025-10-01'],
            'note': ['']
        })
        
        change_path = os.path.join(temp_dir, 'change.csv')
        change_df.to_csv(change_path, index=False)
        
        # Create Excel roster file
        roster_df = pd.DataFrame({
            'provider_npi': ['1234567890'],
            'tax_id': ['123456789'],
            'contract_id': ['C010']
        })
        
        roster_path = os.path.join(temp_dir, 'roster.xlsx')
        roster_df.to_excel(roster_path, index=False)
        
        # Execute
        output_path = check_overlaps(
            change_file=change_path,
            roster_file=roster_path,
            output_dir=temp_dir,
            output_filename='overlap.csv'
        )
        
        # Verify
        assert os.path.exists(output_path)
        result_df = pd.read_csv(output_path)
        assert result_df['NPI_TIN_CID_MATCH'].iloc[0] == True

