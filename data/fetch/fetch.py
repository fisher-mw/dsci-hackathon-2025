import requests
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time

# Create directories
os.makedirs("data/raw", exist_ok=True)

DATASETS = {
    "current_2024_plus": "business-licences",
    "1997_2012": "business-licences-1997-to-2012",
    "2013_2024": "business-licences-2013-to-2024"
}

BASE_URL = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/{}/records"

def fetch_page(dataset_id, offset, limit=100):
    """Fetch a single page of records"""
    url = f"{BASE_URL.format(dataset_id)}?limit={limit}&offset={offset}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("results", []), data.get("total_count", 0)
    except Exception as e:
        print(f"  ⚠️ Error at offset {offset}: {e}")
        return [], 0

def fetch_all_records(dataset_id, max_workers=10):
    """Fetch all records using parallel requests"""
    
    # First request to get total count
    print(f"  Getting total count...", end=" ")
    first_page, total_count = fetch_page(dataset_id, 0)
    print(f"{total_count:,} records to fetch")
    
    if total_count == 0:
        return []
    
    all_records = first_page
    limit = 100
    
    # Calculate all offsets we need to fetch
    offsets = list(range(limit, total_count, limit))
    
    print(f"  Fetching {len(offsets)} pages in parallel with {max_workers} workers...")
    start_time = time()
    
    # Fetch pages in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all fetch tasks
        future_to_offset = {
            executor.submit(fetch_page, dataset_id, offset, limit): offset 
            for offset in offsets
        }
        
        # Collect results as they complete
        completed = 0
        for future in as_completed(future_to_offset):
            records, _ = future.result()
            all_records.extend(records)
            completed += 1
            
            # Progress update every 50 pages
            if completed % 50 == 0:
                elapsed = time() - start_time
                rate = completed / elapsed
                remaining = len(offsets) - completed
                eta = remaining / rate if rate > 0 else 0
                print(f"  Progress: {completed}/{len(offsets)} pages ({len(all_records):,} records) - ETA: {eta:.0f}s")
    
    elapsed = time() - start_time
    print(f"  ✓ Fetched {len(all_records):,} records in {elapsed:.1f}s ({len(all_records)/elapsed:.0f} records/sec)")
    
    return all_records

def fetch_and_save(name, dataset_id, max_workers=10):
    """Fetch data from API and save to CSV"""
    print(f"\nDataset: {name}")
    
    all_records = fetch_all_records(dataset_id, max_workers=max_workers)
    
    if not all_records:
        print(f"  ⚠️ WARNING: No records fetched for {name}")
        return None
    
    df = pd.DataFrame(all_records)
    
    output_path = f"data/raw/{name}.csv"
    df.to_csv(output_path, index=False)
    print(f"  ✓ Saved → {output_path}")
    
    return df

if __name__ == "__main__":
    print("=" * 60)
    print("FAST PARALLEL FETCHING FROM APIs")
    print("=" * 60)
    
    total_start = time()
    
    # Adjust max_workers based on your needs
    # Higher = faster but more aggressive on the API
    # 10-20 is usually a good balance
    MAX_WORKERS = 15
    
    for name, dataset_id in DATASETS.items():
        fetch_and_save(name, dataset_id, max_workers=MAX_WORKERS)
    
    total_elapsed = time() - total_start
    
    print("\n" + "=" * 60)
    print(f"FETCH COMPLETE! Total time: {total_elapsed/60:.1f} minutes")
    print("=" * 60)