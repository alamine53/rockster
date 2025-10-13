"""Pytest configuration and shared fixtures."""

import os
import pytest
import pandas as pd
import tempfile
import shutil
from pathlib import Path


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    tmp = tempfile.mkdtemp()
    yield tmp
    shutil.rmtree(tmp)


@pytest.fixture
def sample_raw_data():
    """Sample raw roster data."""
    return pd.DataFrame({
        'NPI': ['1234567890', '0987654321', '1111111111'],
        'First Name': ['John', 'Jane', 'Bob'],
        'Last Name': ['Doe', 'Smith', 'Johnson'],
        'Middle Initial': ['A', 'B', ''],
        'Degree': ['M.D.', 'D.O.', 'M.D.'],
        'Specialty': ['Cardiology', 'Pediatrics', 'Surgery'],
        'Tax ID': ['123456789', '987654321', '111111111'],
        'Practice Name': ['Heart Care', 'Kids Health', 'Surgical Associates'],
        'Address Line 1': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
        'City': ['Dallas', 'Austin', 'Houston'],
        'State': ['TX', 'TX', 'TX'],
        'Zip': ['75001', '78701', '77001'],
        'Phone': ['214-555-0001', '512-555-0002', '713-555-0003'],
        'Billing Address Line 1': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
        'Billing City': ['Dallas', 'Austin', 'Houston'],
        'Billing State': ['TX', 'TX', 'TX'],
        'Billing Zip': ['75001', '78701', '77001'],
        'Billing NPI': ['1234567890', '0987654321', '1111111111'],
        'Effective Date': ['2025-10-01', '2025-10-15', '2025-11-01'],
        'Action': ['ADD', 'ADD', 'TERM']
    })


@pytest.fixture
def sample_mapping():
    """Sample mapping dictionary."""
    return {
        'provider_npi': 'NPI',
        'first_name': 'First Name',
        'last_name': 'Last Name',
        'middle_initial': 'Middle Initial',
        'degree': 'Degree',
        'specialty_1': 'Specialty',
        'taxonomy_1': 'Specialty',
        'tax_id': 'Tax ID',
        'practice_name': 'Practice Name',
        'address_line1': 'Address Line 1',
        'address_line2': 'Address Line 2',
        'city': 'City',
        'state': 'State',
        'zip_code': 'Zip',
        'phone': 'Phone',
        'billing_address_line1': 'Billing Address Line 1',
        'billing_address_line2': 'Billing Address Line 2',
        'billing_city': 'Billing City',
        'billing_state': 'Billing State',
        'billing_zip': 'Billing Zip',
        'billing_npi': 'Billing NPI',
        'effective_date': 'Effective Date',
        'action': 'Action'
    }


@pytest.fixture
def sample_config_item():
    """Sample configuration item."""
    return {
        'filename': 'test_roster.xlsx',
        'sheet_name': 'Sheet1',
        'mapping_file': 'test_mapping.csv',
        'contract_id': 'C010',
        'action': 'add',
        'note': 'Test note',
        'tag': 'TEST'
    }


@pytest.fixture
def sample_roster_data():
    """Sample existing roster data for overlap checking."""
    return pd.DataFrame({
        'provider_npi': ['1234567890', '9999999999'],
        'tax_id': ['12-3456789', '99-9999999'],
        'contract_id': ['C010', 'C001'],
        'first_name': ['John', 'Alice'],
        'last_name': ['Doe', 'Williams'],
        'effective_date': ['2025-01-01', '2025-01-01'],
        'term_date': ['2026-12-31', '2026-12-31']
    })


@pytest.fixture
def create_test_excel(temp_dir, sample_raw_data):
    """Create a test Excel file."""
    def _create(filename='test_roster.xlsx', sheet_name='Sheet1', data=None):
        if data is None:
            data = sample_raw_data
        filepath = os.path.join(temp_dir, filename)
        data.to_excel(filepath, sheet_name=sheet_name, index=False)
        return filepath
    return _create


@pytest.fixture
def create_test_mapping(temp_dir, sample_mapping):
    """Create a test mapping file."""
    def _create(filename='test_mapping.csv', mapping=None):
        if mapping is None:
            mapping = sample_mapping
        filepath = os.path.join(temp_dir, filename)
        with open(filepath, 'w') as f:
            for canonical, source in mapping.items():
                f.write(f"{canonical},{source}\n")
        return filepath
    return _create

