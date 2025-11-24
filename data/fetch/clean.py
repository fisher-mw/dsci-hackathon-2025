import pandas as pd
import os
import re

os.makedirs("data/cleaned", exist_ok=True)

RAW_FILES = {
    "1997_2012": "data/raw/1997_2012.csv",
    "2013_2024": "data/raw/2013_2024.csv",
    "current_2024_plus": "data/raw/current_2024_plus.csv"
}

COLUMNS_TO_KEEP = [
    "businesstype", "businesssubtype", "status",
    "numberofemployees", "businesstradename",
    "street", "postalcode", "geom"
]

def clean_column_names(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
    )
    return df

def normalize_status(df):
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
    for col in ["businesstype", "businesssubtype"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()
    return df

def extract_lat_lon(df):
    if "geom" not in df.columns:
        return df
    lat_list = []
    lon_list = []
    for g in df["geom"]:
        if isinstance(g, str):
            m = re.search(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", g, re.IGNORECASE)
            if m:
                lon, lat = m.groups()
                lat_list.append(float(lat))
                lon_list.append(float(lon))
                continue
        lat_list.append(None)
        lon_list.append(None)
    df["latitude"] = lat_list
    df["longitude"] = lon_list
    return df

cleaned_dfs = []

for name, file in RAW_FILES.items():
    print(f"Processing {file}...")
    df = pd.read_csv(file, low_memory=False)

    df = clean_column_names(df)

    available = [c for c in COLUMNS_TO_KEEP if c in df.columns]
    df = df[available]

    df = normalize_status(df)
    df = normalize_business_type(df)
    df = extract_lat_lon(df)

    # Add year if issue date exists
    if "issued_date" in df.columns:
        df["year"] = (
            pd.to_datetime(df["issued_date"], errors="coerce")
            .dt.year
        )

    cleaned_dfs.append(df)

merged_df = pd.concat(cleaned_dfs, ignore_index=True)
merged_df.to_csv("data/cleaned/business_licences_1997_2024.csv", index=False)
print("Saved cleaned merged dataset.")
