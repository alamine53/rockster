"""
Roster ingestion pipeline stages.

This package contains the three main stages of roster ingestion:
1. Normalization - standardize raw roster files
2. Aggregation - combine normalized files into a single change file
3. Overlap checking - validate against existing roster
"""

from .normalize import normalize_rosters
from .aggregate import aggregate_rosters
from .overlap_check import check_overlaps

__all__ = ['normalize_rosters', 'aggregate_rosters', 'check_overlaps']

