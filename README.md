# CMIP6 Stat Downscaled Over Italy Download

This workspace contains scripts and notebooks for downloading CMIP6 stat-downscaled data over Italy.

## `data_download.py`

This script downloads climate data from the DDS API.

### Usage

```bash
python data_download.py <out_path> --from-year <year> --to-year <year> --mode <hist|future> [options]
```

### Arguments

- `out_path`: Directory where downloaded data will be saved.
- `--from-year`: Start year for data download.
- `--to-year`: End year for data download (included).
- `--mode`: 'hist' for historical data or 'future' for future projections.
- `--variable`: List of variables to download (default: all).
- `--scenario`: List of scenarios to download (default: 'default' which maps to appropriate scenarios for the mode).
- `--model`: List of models to download (default: all).
- `--dry-run`: Create empty files instead of downloading data (useful for testing).

Run `python data_download.py --help` for more information (like available variables, scenarios and models).

### Example

Download historical data for all models and variables from 1985 to 2014:

```bash
python data_download.py ./data --from-year 1985 --to-year 2014 --mode hist
```
