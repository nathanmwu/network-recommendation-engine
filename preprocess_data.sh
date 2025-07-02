#!/bin/bash

# This script runs all data preprocessing steps in the correct order.
# It ensures that the database and search indexes are fully built from the raw data.

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
# Define the path to the Python executable in the virtual environment.
PYTHON_EXEC="$(pwd)/venv/bin/python"

# --- Main Script ---
echo "--- Starting Data Preprocessing Pipeline ---"

# Step 1: Parse unstructured bios from JSONL into a structured format.
echo "
Step 1/4: Parsing unstructured bios..."
$PYTHON_EXEC ingest/parse_bios.py

# Step 2: Load structured user profiles from CSV into the DuckDB database.
echo "
Step 2/4: Loading structured profiles into DuckDB..."
$PYTHON_EXEC ingest/load_profiles.py

# Step 3: Build the Neo4j knowledge graph from the data in DuckDB.
echo "
Step 3/4: Building the Neo4j knowledge graph..."
$PYTHON_EXEC retrievers/graph_builder.py

# Step 4: Index all user bios for semantic search using Qdrant.
echo "
Step 4/4: Indexing bios for semantic search..."
$PYTHON_EXEC recommenders/semantic_indexer.py

echo "
--- Data Preprocessing Pipeline Complete ---"
