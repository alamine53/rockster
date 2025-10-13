"""Unit tests for src/aggregate.py"""

import pytest
import pandas as pd
import os
from src.aggregate import aggregate_rosters


class TestAggregateRosters:
    """Tests for aggregate_rosters function."""
    
    def test_aggregate_single_file(self, temp_dir):
        """Test aggregating a single file."""
        # Create a test CSV
        df = pd.DataFrame({
            'contract_id': ['C010', 'C010'],
            'action': ['ADD', 'TERM'],
            'provider_npi': ['1234567890', '0987654321'],
            'first_name': ['John', 'Jane'],
            'last_name': ['Doe', 'Smith']
        })
        
        csv_path = os.path.join(temp_dir, 'test.csv')
        df.to_csv(csv_path, index=False)
        
        # Execute
        output_path = aggregate_rosters(
            processed_files=[csv_path],
            output_dir=temp_dir,
            output_filename='aggregated.csv'
        )
        
        # Verify
        assert os.path.exists(output_path)
        result_df = pd.read_csv(output_path)
        assert len(result_df) == 2
    
    def test_aggregate_multiple_files(self, temp_dir):
        """Test aggregating multiple files."""
        # Create multiple test CSVs
        df1 = pd.DataFrame({
            'contract_id': ['C010'],
            'action': ['ADD'],
            'provider_npi': ['1234567890'],
            'first_name': ['John'],
            'last_name': ['Doe']
        })
        
        df2 = pd.DataFrame({
            'contract_id': ['C001'],
            'action': ['TERM'],
            'provider_npi': ['0987654321'],
            'first_name': ['Jane'],
            'last_name': ['Smith']
        })
        
        csv1 = os.path.join(temp_dir, 'test1.csv')
        csv2 = os.path.join(temp_dir, 'test2.csv')
        df1.to_csv(csv1, index=False)
        df2.to_csv(csv2, index=False)
        
        # Execute
        output_path = aggregate_rosters(
            processed_files=[csv1, csv2],
            output_dir=temp_dir,
            output_filename='aggregated.csv'
        )
        
        # Verify
        result_df = pd.read_csv(output_path)
        assert len(result_df) == 2
        assert set(result_df['contract_id']) == {'C010', 'C001'}
    
    def test_aggregate_sorts_data(self, temp_dir):
        """Test that aggregation sorts by contract_id, action, provider_npi."""
        df = pd.DataFrame({
            'contract_id': ['C010', 'C001', 'C010'],
            'action': ['TERM', 'ADD', 'ADD'],
            'provider_npi': ['3333333333', '1111111111', '2222222222'],
            'first_name': ['Alice', 'Bob', 'Charlie'],
            'last_name': ['A', 'B', 'C']
        })
        
        csv_path = os.path.join(temp_dir, 'test.csv')
        df.to_csv(csv_path, index=False)
        
        # Execute
        output_path = aggregate_rosters(
            processed_files=[csv_path],
            output_dir=temp_dir,
            output_filename='aggregated.csv'
        )
        
        # Verify sorting
        result_df = pd.read_csv(output_path)
        assert result_df['contract_id'].iloc[0] == 'C001'
        assert result_df['contract_id'].iloc[1] == 'C010'
        assert result_df['contract_id'].iloc[2] == 'C010'
        # Within C010, ADD should come before TERM
        c010_df = result_df[result_df['contract_id'] == 'C010']
        assert c010_df['action'].iloc[0] == 'ADD'
        assert c010_df['action'].iloc[1] == 'TERM'
    
    def test_aggregate_empty_list_raises_error(self, temp_dir):
        """Test that empty file list raises an error."""
        with pytest.raises(ValueError, match="No processed files"):
            aggregate_rosters(
                processed_files=[],
                output_dir=temp_dir
            )
    
    def test_aggregate_creates_output_dir(self, temp_dir):
        """Test that output directory is created if it doesn't exist."""
        df = pd.DataFrame({
            'contract_id': ['C010'],
            'action': ['ADD'],
            'provider_npi': ['1234567890']
        })
        
        csv_path = os.path.join(temp_dir, 'test.csv')
        df.to_csv(csv_path, index=False)
        
        output_dir = os.path.join(temp_dir, 'new_output')
        assert not os.path.exists(output_dir)
        
        # Execute
        aggregate_rosters(
            processed_files=[csv_path],
            output_dir=output_dir,
            output_filename='aggregated.csv'
        )
        
        # Verify
        assert os.path.exists(output_dir)

