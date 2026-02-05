import os
from pathlib import Path
print(os.environ['RAW_DATA_DIR'])
download_dir = Path(os.environ['RAW_DATA_DIR']) / "ffiec"
import ffiec_data_collector as fdc
import time

downloader = fdc.FFIECDownloader(download_dir=download_dir)

periods = downloader.select_product(fdc.Product.CALL_SINGLE)

results = []
for period in periods:
    
    print(f"Downloading {period.yyyymmdd}...", end=" ")
    result = downloader.download(
        product=fdc.Product.CALL_SINGLE,
        period=period.yyyymmdd,
        format=fdc.FileFormat.XBRL
    )
    results.append(result)
    
    if result.success:
        print(f"✓ ({result.size_bytes:,} bytes)")
    else:
        print(f"✗ Failed: {result.error_message}")
    
    # IMPORTANT: Be respectful to government servers
    # Add delay between requests to avoid overloading the server
    time.sleep(1)  # 1 second delay - adjust as needed

# Summary
successful = sum(1 for r in results if r.success)
print(f"\nCompleted: {successful}/{len(results)} downloads")
