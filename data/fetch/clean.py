import pandas as pd
import os
from time import time

# Create cleaned directory
os.makedirs("data/cleaned", exist_ok=True)

RAW_FILES = {
    "1997_2012": "data/raw/1997_2012.csv",
    "2013_2024": "data/raw/2013_2024.csv",
    "current_2024_plus": "data/raw/current_2024_plus.csv"
}



def clean_column_names(df):
    """Standardize column names"""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
    )
    return df

def normalize_status(df):
    """Normalize status values"""
    if "status" in df.columns:
        df["status"] = (
            df["status"]
            .astype(str)
            .str.lower()
            .str.strip()
            .replace({
                "expired": "closed",
                "closed": "closed",
                "active": "active"
            })
        )
    return df

def normalize_business_type(df):
    """Normalize business type and subtype"""
    for col in ["businesstype", "businesssubtype"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()
    return df

if __name__ == "__main__":
    print("=" * 60)
    print("CLEANING AND MERGING DATA")
    print("=" * 60)
    
    total_start = time()
    cleaned_dfs = []

    for name, file_path in RAW_FILES.items():
        if not os.path.exists(file_path):
            print(f"⚠️ WARNING: {file_path} not found, skipping...")
            continue
        
        file_start = time()
        print(f"\nProcessing {name}...")
        
        # Read with dtype optimization for large files
        df = pd.read_csv(file_path, low_memory=False)
        print(f"  Loaded: {len(df):,} rows, {len(df.columns)} columns ({time()-file_start:.1f}s)")
        
        # Clean column names
        df = clean_column_names(df)
        

        # Normalize data
        df = normalize_status(df)
        df = normalize_business_type(df)
        
        # Add year if issue date exists
        if "issueddate" in df.columns:
            df["year"] = pd.to_datetime(df["issueddate"], errors="coerce").dt.year
        elif "issued_date" in df.columns:
            df["year"] = pd.to_datetime(df["issued_date"], errors="coerce").dt.year
        
        # Replace string 'nan' with actual NA
        df = df.replace('nan', pd.NA)
        
        file_time = time() - file_start
        print(f"  ✓ Cleaned: {len(df):,} records with {len(df.columns)} columns ({file_time:.1f}s)")
        cleaned_dfs.append(df)

    # Merge all dataframes
    if cleaned_dfs:
        print("\nMerging all datasets...", end=" ")
        merge_start = time()
        merged_df = pd.concat(cleaned_dfs, ignore_index=True)
        print(f"done ({time()-merge_start:.1f}s)")
        
        # Save with progress
        output_file = "data/cleaned/business_licences_1997_2024.csv"
        print(f"Saving to {output_file}...", end=" ")
        save_start = time()
        merged_df.to_csv(output_file, index=False)
        print(f"done ({time()-save_start:.1f}s)")

        total_time = time() - total_start

        print("\n" + "=" * 60)
        print("COMPLETE!")
        print("=" * 60)
        print(f"Total records: {len(merged_df):,}")
        print(f"Total columns: {len(merged_df.columns)}")
        print(f"Column names: {list(merged_df.columns)}")
        print(f"Total processing time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
        print(f"\nMemory usage: ~{merged_df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
        print(f"\nData quality:")
        print(f"  Null values per column:")
        for col in merged_df.columns:
            null_count = merged_df[col].isna().sum()
            null_pct = (null_count / len(merged_df)) * 100
            print(f"    {col}: {null_count:,} ({null_pct:.1f}%)")
        
        print("\nSample data:")
        print(merged_df.head())
        
        print("\nValue counts for key columns:")
        if "status" in merged_df.columns:
            print("\nStatus distribution:")
            print(merged_df["status"].value_counts())
        if "businesstype" in merged_df.columns:
            print("\nTop 10 business types:")
            print(merged_df["businesstype"].value_counts().head(10))
    else:
        print("\n⚠️ ERROR: No data to merge!")