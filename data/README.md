# SVK Power Data

This directory contains scraped power consumption and forecast data from SVK (Svenska kraftnät) for the Stockholm (SE3) electricity area.

## Directory Structure

```
data/
├── raw/                    # Raw data files with timestamps (backup)
│   └── svk_power_data_YYYYMMDD_HHMMSS.csv
├── processed/              # Processed and deduplicated data
│   ├── svk_power_data_master.csv      # Main data file (CSV)
│   ├── svk_power_data_master.xlsx     # Main data file (Excel)
│   └── data_summary.txt               # Summary statistics
└── README.md              # This file
```

## Data Files

### Master Data File (`svk_power_data_master.csv`)
The main consolidated dataset containing all historical data scraped from SVK. This file is automatically deduplicated and updated with each scraper run.

**Columns:**
- `Timme` - Hour range (e.g., "00-01", "01-02")
- `Prognos (MW)` - Forecasted power consumption in megawatts
- `Förbrukning (MW)` - Actual power consumption in megawatts
- `Date` - Date in YYYY-MM-DD format
- `DateTime` - Combined date and time stamp

### Raw Data Files
Timestamped backup files created with each scraper run. These are kept for debugging and data recovery purposes.

## Update Schedule

The data is automatically updated daily at 06:00 UTC (08:00 Swedish time) via GitHub Actions.

## Data Quality Notes

- The scraper runs with a delay to ensure data availability on the SVK website
- Duplicate records are automatically removed based on DateTime
- Missing values are represented as NaN in numeric columns
- Historical data might be revised by SVK, so the latest scrape is considered authoritative

## Usage

To use this data in Python:

```python
import pandas as pd

# Load the master data file
df = pd.read_csv('data/processed/svk_power_data_master.csv')

# Convert DateTime to datetime type
df['DateTime'] = pd.to_datetime(df['DateTime'])

# Example: Get data for specific date
date_data = df[df['Date'] == '2024-01-15']

# Example: Calculate daily average consumption
daily_avg = df.groupby('Date')['Förbrukning (MW)'].mean()
```

## Manual Updates

To manually trigger a data update:
1. Go to the Actions tab in the GitHub repository
2. Select "Scrape SVK Power Data" workflow
3. Click "Run workflow"
4. Optionally specify the number of days to scrape and start date

## Contact

For issues or questions about the data, please open an issue in the repository.