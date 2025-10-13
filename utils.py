import csv
import pandas as pd
from datetime import datetime
import dateutil.parser as dtparser


def read_roster_and_apply_mapping(fpath: str, mapping_fpath: str, sheet_name: str = None) -> pd.DataFrame:
    """ read the roster file """
    df = pd.read_excel(fpath, sheet_name=sheet_name)
    mapping = load_mapping(mapping_fpath)
    df = apply_mapping(df, mapping)
    return df

def load_mapping(path: str) -> dict:
    """
    Expects a two-column CSV file with NO header row:
      col A = canonical key (e.g., provider_npi, first_name, address_line1, ...)
      col B = incoming roster column name (e.g., NPI, First Name, Address Line 1, ...)
    We ignore rows with blank key or value.
    """
    mp = {}
    with open(path, 'r', newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) >= 2:
                k = (row[0] or "").strip()
                v = (row[1] or "").strip()
                if k and v:
                    mp[k] = v
    return mp


def concat_full_name(first, middle, last):
    if middle.lower() == "nan":
        middle = ""
    if middle:
        return f"{first} {middle[0].upper()}. {last}"
    else:
        return f"{first} {last}"
    
def build_full_name(row):
    first = str(row.get('first_name', '')).strip()
    middle = str(row.get('middle_initial', '')).strip()
    if middle.lower() == "nan":
        middle = ""
    last = str(row.get('last_name', '')).strip()
    if middle:
        return f"{first} {middle} {last}"
    else:
        return f"{first} {last}"


def format_tax_id(val):
    val = str(val).replace('-', '').zfill(9)
    return f'{val[:2]}-{val[2:]}'


def format_zip_code(val):
    """Format zip code to 5-digit format."""
    s = str(val).strip()
    if s == "" or s.lower() in ("none", "null", "nan"):
        return ""
    
    # Remove any non-digit characters
    digits = ''.join(c for c in s if c.isdigit())
    
    # Take first 5 digits
    if len(digits) >= 5:
        return digits[:5]
    elif len(digits) > 0:
        # If less than 5 digits, zero-pad on the left
        return digits.zfill(5)
    else:
        return ""


def format_phone(val):
    """Format phone number as (xxx) xxx-xxxx."""
    s = str(val).strip()
    if s == "" or s.lower() in ("none", "null", "nan"):
        return ""
    
    # Remove any non-digit characters
    digits = ''.join(c for c in s if c.isdigit())
    
    # Handle different lengths
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == '1':
        # Remove leading 1 for US numbers
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    elif len(digits) >= 10:
        # Use last 10 digits
        return f"({digits[-10:-7]}) {digits[-7:-4]}-{digits[-4:]}"
    else:
        # Return as-is if not enough digits
        return s


def format_state(val):
    """Format state to 2-letter uppercase code."""
    s = str(val).strip().upper()
    if s == "" or s in ("NONE", "NULL", "NAN"):
        return ""
    
    # State name to abbreviation mapping
    state_map = {
        'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
        'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
        'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
        'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
        'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
        'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
        'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
        'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
        'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
        'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
        'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
        'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
        'WISCONSIN': 'WI', 'WYOMING': 'WY', 'DISTRICT OF COLUMBIA': 'DC', 'DC': 'DC',
        'PUERTO RICO': 'PR', 'GUAM': 'GU', 'VIRGIN ISLANDS': 'VI'
    }
    
    # Check if already 2-letter code
    if len(s) == 2 and s.isalpha():
        return s
    
    # Look up full state name
    return state_map.get(s, s)


def format_middle_initial(val):
    """Format middle initial to single uppercase letter."""
    s = str(val).strip().upper()
    if s == "" or s in ("NONE", "NULL", "NAN"):
        return ""
    
    # Take first letter only
    if len(s) > 0 and s[0].isalpha():
        return s[0]
    
    return ""


def format_city(val):
    """Format city name with proper capitalization."""
    s = str(val).strip()
    if s == "" or s.lower() in ("none", "null", "nan"):
        return ""
    
    # Title case (capitalize first letter of each word)
    return s.title()


def format_po_box(val):
    """Format PO Box addresses consistently as 'PO BOX'."""
    s = str(val).strip()
    if s == "" or s.lower() in ("none", "null", "nan"):
        return ""
    
    # Replace various PO Box formats with standard format
    import re
    
    # Pattern to match various PO Box formats
    # P.O. Box, P O Box, PO Box, P.O.Box, etc.
    pattern = r'\b[Pp]\.?\s*[Oo]\.?\s*[Bb][Oo][Xx]\.?\s*'
    
    # Replace with standard format
    result = re.sub(pattern, 'PO BOX ', s)
    
    return result

# apply the mapping
def apply_mapping(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Return a DF where columns are your canonical keys."""
    out = {}
    # remove trailing and leading white spaces
    df.columns = df.columns.str.strip()

    for canon, incoming in mapping.items():
        if incoming in df.columns:
            # Handle case where pandas returns multiple columns with same name
            column_data = df[incoming]
            if isinstance(column_data, pd.DataFrame):
                # If multiple columns with same name, take the first one
                out[canon] = column_data.iloc[:, 0]
            else:
                out[canon] = column_data
        else:
            # If the incoming column is not found, create the canonical column as empty
            out[canon] = [""] * len(df)
    out_df = pd.DataFrame(out).fillna("")
    # Make everything string for easy handling
    for c in out_df.columns:
        out_df[c] = out_df[c].astype(str)
    return out_df

def _to_iso_date(val):
    s = str(val).strip()
    if s == "" or s.lower() in ("none", "null", "nan", "nat"):
        return None
    try:
        return dtparser.parse(s, dayfirst=False).date()
    except Exception:
        try:
            dt = pd.to_datetime(s, errors="coerce")
            if pd.isna(dt):
                return None
            return dt.date()
        except Exception:
            return None
    
if __name__ == "__main__":
    mp = load_mapping("data/htpn_mapping_adds.csv")
    print(mp)