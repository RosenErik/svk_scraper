#!/usr/bin/env python3
"""
run_scraper.py - Main script to run the SVK scraper and manage data
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

# Add parent directory to path to import the scraper
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from svk_scraper import SVKPowerScraper


def setup_logging():
    """Configure logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('scraper.log')
        ]
    )
    return logging.getLogger(__name__)


def ensure_data_directory():
    """Create data directory structure if it doesn't exist."""
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Create subdirectories for organization
    (data_dir / "raw").mkdir(exist_ok=True)
    (data_dir / "processed").mkdir(exist_ok=True)
    
    return data_dir


def load_existing_data(data_dir: Path) -> pd.DataFrame:
    """
    Load existing master data file if it exists.
    
    Args:
        data_dir: Path to data directory
        
    Returns:
        DataFrame with existing data or empty DataFrame
    """
    master_file = data_dir / "processed" / "svk_power_data_master.csv"
    
    if master_file.exists():
        logger = logging.getLogger(__name__)
        logger.info(f"Loading existing data from {master_file}")
        
        df = pd.read_csv(master_file)
        
        # Ensure DateTime column is datetime type
        if 'DateTime' in df.columns:
            df['DateTime'] = pd.to_datetime(df['DateTime'])
            
        logger.info(f"Loaded {len(df)} existing records")
        return df
    else:
        logger = logging.getLogger(__name__)
        logger.info("No existing master file found, starting fresh")
        return pd.DataFrame()


def merge_and_deduplicate(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge new data with existing data and remove duplicates.
    
    Args:
        existing_df: Existing data
        new_df: Newly scraped data
        
    Returns:
        Merged and deduplicated DataFrame
    """
    logger = logging.getLogger(__name__)
    
    if existing_df.empty:
        logger.info("No existing data, using new data as is")
        return new_df
    
    if new_df.empty:
        logger.warning("No new data to merge")
        return existing_df
    
    # Combine dataframes
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    
    # Identify duplicate columns for deduplication
    # Use DateTime and Timme (hour) as unique identifiers
    duplicate_cols = []
    if 'DateTime' in combined_df.columns:
        duplicate_cols.append('DateTime')
    elif 'Date' in combined_df.columns and 'Timme' in combined_df.columns:
        duplicate_cols.extend(['Date', 'Timme'])
    
    if duplicate_cols:
        initial_count = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=duplicate_cols, keep='last')
        removed_count = initial_count - len(combined_df)
        
        if removed_count > 0:
            logger.info(f"Removed {removed_count} duplicate records")
    
    # Sort by DateTime if available
    if 'DateTime' in combined_df.columns:
        combined_df = combined_df.sort_values('DateTime')
    
    return combined_df


def save_data(df: pd.DataFrame, data_dir: Path, timestamp: str):
    """
    Save data to multiple formats.
    
    Args:
        df: DataFrame to save
        data_dir: Data directory path
        timestamp: Timestamp string for raw file naming
    """
    logger = logging.getLogger(__name__)
    
    if df.empty:
        logger.warning("No data to save")
        return
    
    # Save raw data with timestamp (for backup/debugging)
    raw_file = data_dir / "raw" / f"svk_power_data_{timestamp}.csv"
    df.to_csv(raw_file, index=False)
    logger.info(f"Saved raw data to {raw_file}")
    
    # Save master file (main file that gets updated)
    master_file = data_dir / "processed" / "svk_power_data_master.csv"
    df.to_csv(master_file, index=False)
    logger.info(f"Saved master data to {master_file} ({len(df)} total records)")
    
    # Also save as Excel for convenience
    excel_file = data_dir / "processed" / "svk_power_data_master.xlsx"
    df.to_excel(excel_file, index=False, engine='openpyxl')
    logger.info(f"Saved Excel file to {excel_file}")
    
    # Create a summary file with latest statistics
    create_summary(df, data_dir)


def create_summary(df: pd.DataFrame, data_dir: Path):
    """
    Create a summary file with statistics about the data.
    
    Args:
        df: DataFrame with all data
        data_dir: Data directory path
    """
    logger = logging.getLogger(__name__)
    
    summary_file = data_dir / "processed" / "data_summary.txt"
    
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write("SVK Power Data Summary\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        f.write(f"Total records: {len(df)}\n")
        
        if 'Date' in df.columns:
            unique_dates = df['Date'].nunique()
            f.write(f"Unique dates: {unique_dates}\n")
            
            if not df.empty:
                f.write(f"Date range: {df['Date'].min()} to {df['Date'].max()}\n")
        
        if 'DateTime' in df.columns and not df.empty:
            f.write(f"Latest data point: {df['DateTime'].max()}\n")
        
        f.write("\n")
        f.write("Columns in dataset:\n")
        for col in df.columns:
            non_null = df[col].notna().sum()
            f.write(f"  - {col}: {non_null}/{len(df)} non-null values\n")
        
        # Add some basic statistics for numeric columns
        numeric_cols = ['Prognos (MW)', 'Förbrukning (MW)']
        f.write("\nNumeric column statistics:\n")
        for col in numeric_cols:
            if col in df.columns:
                f.write(f"\n{col}:\n")
                f.write(f"  Mean: {df[col].mean():.2f}\n")
                f.write(f"  Min: {df[col].min():.2f}\n")
                f.write(f"  Max: {df[col].max():.2f}\n")
    
    logger.info(f"Created summary file: {summary_file}")


def calculate_days_to_scrape(existing_df: pd.DataFrame, default_days: int = 3) -> int:
    """
    Calculate how many days we should scrape based on existing data.
    
    Args:
        existing_df: Existing data
        default_days: Default number of days to scrape
        
    Returns:
        Number of days to scrape
    """
    if existing_df.empty:
        # No existing data, scrape more days initially
        return max(default_days, 7)
    
    # Check the most recent date in existing data
    if 'Date' in existing_df.columns:
        latest_date = pd.to_datetime(existing_df['Date'].max())
        days_since = (datetime.now() - latest_date).days
        
        # If data is very old, scrape more days
        if days_since > 7:
            return min(days_since + 2, 30)  # Cap at 30 days
    
    return default_days


def main():
    """Main execution function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run SVK power scraper')
    parser.add_argument('--days', type=int, default=3,
                       help='Number of days to scrape (default: 3)')
    parser.add_argument('--start-date', type=str, default=None,
                       help='Start date in YYYY-MM-DD format (optional)')
    parser.add_argument('--headless', action='store_true', default=True,
                       help='Run browser in headless mode (default: True)')
    
    args = parser.parse_args()
    
    # Setup
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("Starting SVK Power Data Scraper")
    logger.info("=" * 60)
    
    # Ensure data directory exists
    data_dir = ensure_data_directory()
    
    # Load existing data
    existing_df = load_existing_data(data_dir)
    
    # Adjust days to scrape based on existing data
    days_to_scrape = calculate_days_to_scrape(existing_df, args.days)
    if days_to_scrape != args.days:
        logger.info(f"Adjusted scraping from {args.days} to {days_to_scrape} days based on existing data")
    
    # Run the scraper
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    try:
        with SVKPowerScraper(headless=args.headless) as scraper:
            logger.info(f"Scraping {days_to_scrape} days of data...")
            if args.start_date:
                logger.info(f"Starting from date: {args.start_date}")
            
            new_df = scraper.scrape_multiple_days(
                num_days=days_to_scrape,
                start_date=args.start_date
            )
            
            if not new_df.empty:
                logger.info(f"Successfully scraped {len(new_df)} new records")
                
                # Merge with existing data and remove duplicates
                final_df = merge_and_deduplicate(existing_df, new_df)
                
                # Save the data
                save_data(final_df, data_dir, timestamp)
                
                # Report statistics
                new_records = len(final_df) - len(existing_df)
                if new_records > 0:
                    logger.info(f"✅ Added {new_records} new records to dataset")
                else:
                    logger.info("ℹ️ No new unique records added (all were duplicates)")
                    
                logger.info(f"📊 Total records in dataset: {len(final_df)}")
            else:
                logger.warning("No data was scraped")
                
    except Exception as e:
        logger.error(f"Scraper failed with error: {e}", exc_info=True)
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("Scraper completed successfully")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()