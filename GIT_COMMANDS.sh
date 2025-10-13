#!/bin/bash
# Quick Git Setup for Rockster Pipeline

echo "=== Git Setup for Rockster Pipeline ==="
echo ""

# Step 1: Initialize (if needed)
if [ ! -d .git ]; then
    echo "Step 1: Initializing git repository..."
    git init
else
    echo "Step 1: Git repository already initialized ✓"
fi

# Step 2: Configure (optional)
echo ""
echo "Step 2: Configure git (optional)"
echo "  git config user.name \"Your Name\""
echo "  git config user.email \"your.email@example.com\""
echo ""

# Step 3: Check status
echo "Step 3: Checking what will be committed..."
git status

echo ""
echo "=== Ready to commit! ==="
echo ""
echo "Next steps:"
echo "  1. Review the files above"
echo "  2. git add ."
echo "  3. git commit -m 'Initial commit: Roster ingestion pipeline'"
echo "  4. git remote add origin <your-github-url>"
echo "  5. git push -u origin main"
echo ""
