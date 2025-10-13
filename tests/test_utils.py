"""Unit tests for utils.py"""

import pytest
import pandas as pd
from utils import (
    load_mapping,
    apply_mapping,
    concat_full_name,
    build_full_name,
    format_tax_id,
    format_zip_code,
    format_phone,
    format_state,
    format_middle_initial,
    format_city,
    format_po_box,
    _to_iso_date
)


class TestLoadMapping:
    """Tests for load_mapping function."""
    
    def test_load_mapping_basic(self, create_test_mapping):
        """Test loading a basic mapping file."""
        mapping_file = create_test_mapping()
        mapping = load_mapping(mapping_file)
        
        assert isinstance(mapping, dict)
        assert 'provider_npi' in mapping
        assert mapping['provider_npi'] == 'NPI'
    
    def test_load_mapping_ignores_blank_lines(self, temp_dir):
        """Test that blank lines are ignored."""
        filepath = f"{temp_dir}/mapping.csv"
        with open(filepath, 'w') as f:
            f.write("provider_npi,NPI\n")
            f.write("\n")
            f.write("first_name,First Name\n")
            f.write(",,\n")
        
        mapping = load_mapping(filepath)
        assert len(mapping) == 2
        assert mapping['provider_npi'] == 'NPI'
        assert mapping['first_name'] == 'First Name'


class TestApplyMapping:
    """Tests for apply_mapping function."""
    
    def test_apply_mapping_basic(self, sample_raw_data, sample_mapping):
        """Test basic mapping application."""
        mapped_df = apply_mapping(sample_raw_data, sample_mapping)
        
        assert 'provider_npi' in mapped_df.columns
        assert 'first_name' in mapped_df.columns
        assert mapped_df['provider_npi'].iloc[0] == '1234567890'
    
    def test_apply_mapping_missing_columns(self, sample_raw_data, sample_mapping):
        """Test mapping with missing columns creates empty columns."""
        mapping = sample_mapping.copy()
        mapping['missing_field'] = 'NonexistentColumn'
        
        mapped_df = apply_mapping(sample_raw_data, mapping)
        
        assert 'missing_field' in mapped_df.columns
        assert all(mapped_df['missing_field'] == '')
    
    def test_apply_mapping_all_strings(self, sample_raw_data, sample_mapping):
        """Test that all output columns are strings."""
        mapped_df = apply_mapping(sample_raw_data, sample_mapping)
        
        for col in mapped_df.columns:
            assert mapped_df[col].dtype == 'object'


class TestConcatFullName:
    """Tests for concat_full_name function."""
    
    def test_concat_with_middle_initial(self):
        """Test concatenation with middle initial."""
        result = concat_full_name('John', 'A', 'Doe')
        assert result == 'John A. Doe'
    
    def test_concat_without_middle_initial(self):
        """Test concatenation without middle initial."""
        result = concat_full_name('John', '', 'Doe')
        assert result == 'John Doe'
    
    def test_concat_with_nan_middle_initial(self):
        """Test concatenation with 'nan' string as middle initial."""
        result = concat_full_name('John', 'nan', 'Doe')
        assert result == 'John Doe'


class TestFormatTaxId:
    """Tests for format_tax_id function."""
    
    def test_format_tax_id_9_digits(self):
        """Test formatting 9-digit tax ID."""
        result = format_tax_id('123456789')
        assert result == '12-3456789'
    
    def test_format_tax_id_with_dash(self):
        """Test formatting tax ID that already has dash."""
        result = format_tax_id('12-3456789')
        assert result == '12-3456789'
    
    def test_format_tax_id_short_number(self):
        """Test formatting short number (zero-pads to 9 digits)."""
        result = format_tax_id('12345')
        assert result == '00-0012345'
    
    def test_format_tax_id_handles_int(self):
        """Test formatting integer tax ID."""
        result = format_tax_id(123456789)
        assert result == '12-3456789'


class TestFormatZipCode:
    """Tests for format_zip_code function."""
    
    def test_format_zip_5_digits(self):
        """Test formatting 5-digit zip."""
        result = format_zip_code('75001')
        assert result == '75001'
    
    def test_format_zip_extended(self):
        """Test formatting extended zip (9 digits)."""
        result = format_zip_code('75001-1234')
        assert result == '75001'
    
    def test_format_zip_with_spaces(self):
        """Test formatting zip with spaces."""
        result = format_zip_code(' 75001 ')
        assert result == '75001'
    
    def test_format_zip_short(self):
        """Test formatting short zip (zero-pads)."""
        result = format_zip_code('123')
        assert result == '00123'
    
    def test_format_zip_empty(self):
        """Test formatting empty zip."""
        result = format_zip_code('')
        assert result == ''


class TestFormatPhone:
    """Tests for format_phone function."""
    
    def test_format_phone_10_digits(self):
        """Test formatting 10-digit phone."""
        result = format_phone('2145551234')
        assert result == '(214) 555-1234'
    
    def test_format_phone_with_formatting(self):
        """Test formatting already formatted phone."""
        result = format_phone('(214) 555-1234')
        assert result == '(214) 555-1234'
    
    def test_format_phone_with_dashes(self):
        """Test formatting phone with dashes."""
        result = format_phone('214-555-1234')
        assert result == '(214) 555-1234'
    
    def test_format_phone_11_digits(self):
        """Test formatting 11-digit phone (with leading 1)."""
        result = format_phone('12145551234')
        assert result == '(214) 555-1234'
    
    def test_format_phone_empty(self):
        """Test formatting empty phone."""
        result = format_phone('')
        assert result == ''
    
    def test_format_phone_short(self):
        """Test formatting short phone (returns as-is)."""
        result = format_phone('555-1234')
        assert result == '555-1234'


class TestFormatState:
    """Tests for format_state function."""
    
    def test_format_state_full_name(self):
        """Test formatting full state name."""
        result = format_state('Texas')
        assert result == 'TX'
    
    def test_format_state_uppercase_name(self):
        """Test formatting uppercase state name."""
        result = format_state('CALIFORNIA')
        assert result == 'CA'
    
    def test_format_state_already_abbreviated(self):
        """Test state already abbreviated."""
        result = format_state('TX')
        assert result == 'TX'
    
    def test_format_state_lowercase_abbrev(self):
        """Test lowercase abbreviation."""
        result = format_state('tx')
        assert result == 'TX'
    
    def test_format_state_multi_word(self):
        """Test multi-word state name."""
        result = format_state('New York')
        assert result == 'NY'
    
    def test_format_state_empty(self):
        """Test empty state."""
        result = format_state('')
        assert result == ''


class TestFormatMiddleInitial:
    """Tests for format_middle_initial function."""
    
    def test_format_middle_initial_single_letter(self):
        """Test single letter middle initial."""
        result = format_middle_initial('A')
        assert result == 'A'
    
    def test_format_middle_initial_full_name(self):
        """Test full middle name returns first letter."""
        result = format_middle_initial('Michael')
        assert result == 'M'
    
    def test_format_middle_initial_lowercase(self):
        """Test lowercase returns uppercase."""
        result = format_middle_initial('j')
        assert result == 'J'
    
    def test_format_middle_initial_empty(self):
        """Test empty middle initial."""
        result = format_middle_initial('')
        assert result == ''


class TestFormatCity:
    """Tests for format_city function."""
    
    def test_format_city_lowercase(self):
        """Test lowercase city name."""
        result = format_city('fort worth')
        assert result == 'Fort Worth'
    
    def test_format_city_uppercase(self):
        """Test uppercase city name."""
        result = format_city('DALLAS')
        assert result == 'Dallas'
    
    def test_format_city_mixed_case(self):
        """Test mixed case city name."""
        result = format_city('sAn AnToNiO')
        assert result == 'San Antonio'
    
    def test_format_city_empty(self):
        """Test empty city."""
        result = format_city('')
        assert result == ''


class TestFormatPOBox:
    """Tests for format_po_box function."""
    
    def test_format_po_box_with_periods(self):
        """Test P.O. Box format."""
        result = format_po_box('P.O. Box 1234')
        assert result == 'PO BOX 1234'
    
    def test_format_po_box_with_spaces(self):
        """Test P O Box format."""
        result = format_po_box('P O Box 5678')
        assert result == 'PO BOX 5678'
    
    def test_format_po_box_standard(self):
        """Test standard PO Box format."""
        result = format_po_box('PO Box 9999')
        assert result == 'PO BOX 9999'
    
    def test_format_po_box_lowercase(self):
        """Test lowercase po box."""
        result = format_po_box('p.o. box 1111')
        assert result == 'PO BOX 1111'
    
    def test_format_po_box_no_space_after_box(self):
        """Test P.O.Box without space."""
        result = format_po_box('P.O.Box 2222')
        assert result == 'PO BOX 2222'
    
    def test_format_po_box_regular_address(self):
        """Test regular address (no change)."""
        result = format_po_box('123 Main Street')
        assert result == '123 Main Street'
    
    def test_format_po_box_empty(self):
        """Test empty address."""
        result = format_po_box('')
        assert result == ''


class TestToIsoDate:
    """Tests for _to_iso_date function."""
    
    def test_to_iso_date_yyyy_mm_dd(self):
        """Test parsing YYYY-MM-DD format."""
        from datetime import date
        result = _to_iso_date('2025-10-15')
        assert result == date(2025, 10, 15)
    
    def test_to_iso_date_mm_dd_yyyy(self):
        """Test parsing MM/DD/YYYY format."""
        from datetime import date
        result = _to_iso_date('10/15/2025')
        assert result == date(2025, 10, 15)
    
    def test_to_iso_date_empty_string(self):
        """Test parsing empty string."""
        result = _to_iso_date('')
        assert result is None
    
    def test_to_iso_date_none(self):
        """Test parsing None."""
        result = _to_iso_date('None')
        assert result is None
    
    def test_to_iso_date_nan(self):
        """Test parsing 'nan' string."""
        result = _to_iso_date('nan')
        assert result is None

