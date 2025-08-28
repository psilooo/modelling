from pathlib import Path
import pyarrow.parquet as pq

BASE_DIR = Path('/content/drive/MyDrive/binance_data')

if not BASE_DIR.exists():
    print(f'Base directory {BASE_DIR} does not exist')
else:
    for folder in sorted(BASE_DIR.iterdir()):
        if not folder.is_dir():
            continue
        files = sorted(folder.glob('*.parquet'))
        if not files:
            print(f'No parquet files found in {folder.name}')
            continue
        first_file = files[0]
        pf = pq.ParquetFile(first_file)
        table = pf.read_row_groups([0]).slice(0, 2)
        df = table.to_pandas()
        print(f'=== {folder.name} : {first_file.name} ===')
        print(df)
        print()
