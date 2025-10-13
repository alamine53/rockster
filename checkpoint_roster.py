#!/usr/bin/env python3
"""
Roster Checkpoint Utility

Create and manage roster checkpoints/snapshots for version control.
"""

import argparse
import shutil
import os
from datetime import datetime
import json


def create_checkpoint(roster_file: str, checkpoint_dir: str = "rosters/checkpoints", label: str = None):
    """
    Create a checkpoint of the current roster.
    
    Args:
        roster_file: Path to the roster file to checkpoint
        checkpoint_dir: Directory to store checkpoints
        label: Optional label for the checkpoint (e.g., 'before-oct-update')
    """
    if not os.path.exists(roster_file):
        raise FileNotFoundError(f"Roster file not found: {roster_file}")
    
    # Create checkpoint directory if it doesn't exist
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    # Generate checkpoint filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_name = os.path.splitext(os.path.basename(roster_file))[0]
    ext = os.path.splitext(roster_file)[1]
    
    if label:
        checkpoint_name = f"{base_name}_{label}_{timestamp}{ext}"
    else:
        checkpoint_name = f"{base_name}_checkpoint_{timestamp}{ext}"
    
    checkpoint_path = os.path.join(checkpoint_dir, checkpoint_name)
    
    # Copy the file
    shutil.copy2(roster_file, checkpoint_path)
    
    # Create metadata file
    metadata = {
        'checkpoint_name': checkpoint_name,
        'original_file': roster_file,
        'created_at': datetime.now().isoformat(),
        'label': label,
        'file_size': os.path.getsize(checkpoint_path)
    }
    
    metadata_path = checkpoint_path.replace(ext, '.json')
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"✓ Checkpoint created: {checkpoint_name}")
    print(f"  Location: {checkpoint_path}")
    print(f"  Size: {metadata['file_size']:,} bytes")
    if label:
        print(f"  Label: {label}")
    
    return checkpoint_path


def list_checkpoints(checkpoint_dir: str = "rosters/checkpoints"):
    """List all available checkpoints."""
    if not os.path.exists(checkpoint_dir):
        print(f"No checkpoints directory found: {checkpoint_dir}")
        return
    
    checkpoints = []
    for file in os.listdir(checkpoint_dir):
        if file.endswith('.json'):
            metadata_path = os.path.join(checkpoint_dir, file)
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            checkpoints.append(metadata)
    
    if not checkpoints:
        print("No checkpoints found")
        return
    
    # Sort by creation time
    checkpoints.sort(key=lambda x: x['created_at'], reverse=True)
    
    print(f"\n{'='*80}")
    print(f"AVAILABLE CHECKPOINTS ({len(checkpoints)})")
    print(f"{'='*80}\n")
    
    for i, cp in enumerate(checkpoints, 1):
        created = datetime.fromisoformat(cp['created_at'])
        print(f"{i}. {cp['checkpoint_name']}")
        print(f"   Created: {created.strftime('%Y-%m-%d %H:%M:%S')}")
        if cp.get('label'):
            print(f"   Label: {cp['label']}")
        print(f"   Size: {cp['file_size']:,} bytes")
        print()


def restore_checkpoint(checkpoint_name: str, checkpoint_dir: str = "rosters/checkpoints", 
                      output_path: str = None):
    """
    Restore a roster from a checkpoint.
    
    Args:
        checkpoint_name: Name of the checkpoint file or pattern to match
        checkpoint_dir: Directory containing checkpoints
        output_path: Where to restore the file (defaults to output/ directory)
    """
    # Find the checkpoint file
    checkpoint_path = None
    
    if os.path.exists(checkpoint_name):
        # Full path provided
        checkpoint_path = checkpoint_name
    else:
        # Search in checkpoint directory
        for file in os.listdir(checkpoint_dir):
            if checkpoint_name in file and not file.endswith('.json'):
                checkpoint_path = os.path.join(checkpoint_dir, file)
                break
    
    if not checkpoint_path or not os.path.exists(checkpoint_path):
        raise FileNotFoundError(f"Checkpoint not found: {checkpoint_name}")
    
    # Determine output path
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d')
        base_name = os.path.basename(checkpoint_path)
        output_path = os.path.join("output", f"{timestamp}_restored_{base_name}")
    
    # Copy the checkpoint
    shutil.copy2(checkpoint_path, output_path)
    
    print(f"✓ Checkpoint restored")
    print(f"  From: {checkpoint_path}")
    print(f"  To: {output_path}")
    
    return output_path


def main():
    parser = argparse.ArgumentParser(
        description='Manage roster checkpoints',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Create a checkpoint:
    python checkpoint_roster.py create output/20250923_MCC_FullRoster.xlsx
    python checkpoint_roster.py create output/20250923_MCC_FullRoster.xlsx -l before-oct-update
  
  List checkpoints:
    python checkpoint_roster.py list
  
  Restore a checkpoint:
    python checkpoint_roster.py restore MCC_FullRoster_before-oct-update
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Create checkpoint
    create_parser = subparsers.add_parser('create', help='Create a checkpoint')
    create_parser.add_argument('roster_file', help='Path to roster file')
    create_parser.add_argument('-l', '--label', help='Label for the checkpoint')
    create_parser.add_argument('-d', '--dir', default='rosters/checkpoints',
                              help='Checkpoint directory (default: rosters/checkpoints)')
    
    # List checkpoints
    list_parser = subparsers.add_parser('list', help='List all checkpoints')
    list_parser.add_argument('-d', '--dir', default='rosters/checkpoints',
                            help='Checkpoint directory (default: rosters/checkpoints)')
    
    # Restore checkpoint
    restore_parser = subparsers.add_parser('restore', help='Restore a checkpoint')
    restore_parser.add_argument('checkpoint', help='Checkpoint name or pattern')
    restore_parser.add_argument('-o', '--output', help='Output path for restored file')
    restore_parser.add_argument('-d', '--dir', default='rosters/checkpoints',
                               help='Checkpoint directory (default: rosters/checkpoints)')
    
    args = parser.parse_args()
    
    if args.command == 'create':
        create_checkpoint(args.roster_file, args.dir, args.label)
    elif args.command == 'list':
        list_checkpoints(args.dir)
    elif args.command == 'restore':
        restore_checkpoint(args.checkpoint, args.dir, args.output)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

