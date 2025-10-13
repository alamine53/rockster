# Deprecated Files

This directory contains legacy files that have been superseded by the refactored architecture.

## Files

- `ingest.py` - Old entry point for roster ingestion. Replaced by the modular `stages/` architecture.
- `check_overlap.py` - Standalone overlap checking script. Replaced by `stages/overlap_check.py`.

## Why these files are deprecated

The codebase has been refactored to follow a clean, three-stage pipeline architecture:
1. Normalization (stages/normalize.py)
2. Aggregation (stages/aggregate.py)
3. Overlap Checking (stages/overlap_check.py)

The new architecture provides:
- Clear separation of concerns
- Better error handling
- Comprehensive logging
- Reusable modules
- Consistent CLI interface through main.py

These files are kept for reference only and should not be used in production workflows.

