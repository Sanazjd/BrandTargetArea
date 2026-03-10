"""
Convert CSV files to Parquet format for improved performance.
Parquet files are columnar and compressed, resulting in faster read times.
"""

import pandas as pd
import os
from pathlib import Path

# Define the base directory
BASE_DIR = Path(__file__).parent

# List of CSV files to convert
CSV_FILES = [
    "NA_market_P4_breeding_locationsII.csv",
    "FV_brand_acreage.csv",
]

def convert_csv_to_parquet(csv_filename: str) -> None:
    """Convert a single CSV file to Parquet format."""
    csv_path = BASE_DIR / csv_filename
    parquet_filename = csv_filename.replace(".csv", ".parquet")
    parquet_path = BASE_DIR / parquet_filename
    
    if not csv_path.exists():
        print(f"Warning: {csv_filename} not found, skipping...")
        return
    
    print(f"Converting {csv_filename} to {parquet_filename}...")
    
    # Read CSV
    df = pd.read_csv(csv_path)
    
    # Write to Parquet with compression
    df.to_parquet(parquet_path, engine="pyarrow", compression="snappy", index=False)
    
    # Print file size comparison
    csv_size = csv_path.stat().st_size / (1024 * 1024)  # MB
    parquet_size = parquet_path.stat().st_size / (1024 * 1024)  # MB
    reduction = ((csv_size - parquet_size) / csv_size) * 100
    
    print(f"  CSV size: {csv_size:.2f} MB")
    print(f"  Parquet size: {parquet_size:.2f} MB")
    print(f"  Size reduction: {reduction:.1f}%")
    print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
    print()

def main():
    print("=" * 50)
    print("CSV to Parquet Conversion")
    print("=" * 50)
    print()
    
    for csv_file in CSV_FILES:
        convert_csv_to_parquet(csv_file)
    
    print("Conversion complete!")
    print("You can now use the Parquet files in your R application.")

if __name__ == "__main__":
    main()
