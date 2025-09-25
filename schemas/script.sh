#!/bin/bash

# Exit immediately on error
set -e
INPUT_DIR="./schemas"
OUTPUT_DIR="./compiled_schemas"

# Create the output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Compile all .fbs files in the input directory for Python
for fbs_file in "$INPUT_DIR"/*.fbs; do
  if [ -f "$fbs_file" ]; then
    echo "Compiling $fbs_file..."
    flatc -o "$OUTPUT_DIR" --python "$fbs_file"
  fi
done

echo "Python FlatBuffer compilation complete. Files are in $OUTPUT_DIR."
