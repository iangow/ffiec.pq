# Package index

## Configuration

- [`ffiec_raw_data_dir()`](https://iangow.github.io/ffiec.pq/reference/ffiec_raw_data_dir.md)
  : Resolve the FFIEC raw data repository directory
- [`ffiec_data_dir()`](https://iangow.github.io/ffiec.pq/reference/ffiec_data_dir.md)
  : Resolve the FFIEC Parquet data repository directory

## Processing data

- [`ffiec_process()`](https://iangow.github.io/ffiec.pq/reference/ffiec_process.md)
  : Process FFIEC call report bulk data
- [`ffiec_process_xbrls()`](https://iangow.github.io/ffiec.pq/reference/ffiec_process_xbrls.md)
  : Process FFIEC XBRL bulk zip files
- [`ffiec_list_zips()`](https://iangow.github.io/ffiec.pq/reference/ffiec_list_zips.md)
  : List FFIEC bulk zip files

## Reading Parquet data

- [`ffiec_scan_pqs()`](https://iangow.github.io/ffiec.pq/reference/ffiec_scan_pqs.md)
  : Scan FFIEC Parquet files into DuckDB
- [`ffiec_list_pqs()`](https://iangow.github.io/ffiec.pq/reference/ffiec_list_pqs.md)
  : List generated FFIEC Parquet files
- [`ffiec_pivot()`](https://iangow.github.io/ffiec.pq/reference/ffiec_pivot.md)
  : Pivot long FFIEC data to wide format using DuckDB
- [`ffiec_check_pq_keys()`](https://iangow.github.io/ffiec.pq/reference/ffiec_check_pq_keys.md)
  : Check primary-key and non-NULL constraints in FFIEC Parquet files

## Metadata

- [`ffiec_items`](https://iangow.github.io/ffiec.pq/reference/ffiec_items.md)
  : FFIEC item master table
- [`ffiec_item_details`](https://iangow.github.io/ffiec.pq/reference/ffiec_item_details.md)
  : FFIEC item metadata and reporting details
