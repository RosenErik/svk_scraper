#!/usr/bin/env python3
"""
svk_scraper.py - Main scraper class for SVK power data
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
from datetime import datetime
import locale
from typing import Tuple, Optional
import logging
import os


class SVKPowerScraper:
    """Scraper for SVK power system data from kontrollrummet."""
    
    def __init__(self, headless: bool = True):
        """
        Initialize the scraper.
        
        Args:
            headless: Run browser in headless mode if True
        """
        self.driver = None
        self.wait = None
        self.headless = headless
        self.base_url = "https://www.svk.se/om-kraftsystemet/kontrollrummet/"
        self.logger = logging.getLogger(__name__)
        
    def __enter__(self):
        """Context manager entry."""
        self.initialize_driver()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures driver is closed."""
        if self.driver:
            self.driver.quit()
            
    def initialize_driver(self):
        """Initialize the Chrome driver using webdriver-manager."""
        options = Options()
        if self.headless:
            options.add_argument("--headless")
        
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        try:
            # Use webdriver-manager to handle driver installation
            self.logger.info("Initializing driver with webdriver-manager...")
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            self.logger.info("✓ Chrome driver initialized successfully")

        except Exception as e:
            self.logger.error(f"Could not initialize driver using webdriver-manager: {e}", exc_info=True)
            raise
            
        self.driver.set_page_load_timeout(30)
        self.wait = WebDriverWait(self.driver, 15)
        
        # Navigate to the page
        self.logger.info(f"Navigating to {self.base_url}")
        self.driver.get(self.base_url)
        time.sleep(3)  # Initial load time
        
    def accept_cookies(self) -> None:
        """Accept cookie consent if present."""
        try:
            accept_button = self.wait.until(
                EC.element_to_be_clickable((By.CLASS_NAME, "cookie-accept-all"))
            )
            accept_button.click()
            time.sleep(1)
            self.logger.info("✓ Cookies accepted")
        except Exception as e:
            self.logger.debug(f"Cookie banner not found or already accepted: {e}")
            
    def select_stockholm_tab(self) -> None:
        """Select the Stockholm (SE3) electricity area tab."""
        try:
            # Wait for tabs to be present
            time.sleep(2)
            
            # Find and click Stockholm tab
            tab_button = self.wait.until(
                EC.element_to_be_clickable((
                    By.XPATH, 
                    "//button[contains(@class, 'custom-trigger') and contains(text(), 'Elområde Stockholm (SE3)')]"
                ))
            )
            
            # Scroll into view and click
            self.driver.execute_script("arguments[0].scrollIntoView(true);", tab_button)
            time.sleep(0.5)
            tab_button.click()
            time.sleep(2)
            self.logger.info("✓ Stockholm (SE3) tab selected")
            
        except Exception as e:
            self.logger.error(f"Error selecting Stockholm tab: {e}")
            raise
            
    def select_table_view(self) -> None:
        """Switch to table view."""
        try:
            # Wait for page to stabilize
            time.sleep(2)
            
            # Find all buttons containing "Tabell"
            self.wait.until(
                EC.presence_of_all_elements_located((By.XPATH, "//button[contains(., 'Tabell')]"))
            )
            
            buttons = self.driver.find_elements(By.XPATH, "//button[contains(., 'Tabell')]")
            
            # Find the correct table button
            table_button_clicked = False
            for button in buttons:
                try:
                    # Check if this is the right button
                    aria_selected = button.get_attribute("aria-selected")
                    if aria_selected == "false" and "Tabell" in button.text:
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", button)
                        time.sleep(0.5)
                        button.click()
                        table_button_clicked = True
                        time.sleep(2)
                        self.logger.info("✓ Table view selected")
                        break
                except:
                    continue
                    
            if not table_button_clicked:
                self.logger.info("Table view might already be selected")
                
        except Exception as e:
            self.logger.warning(f"Error selecting table view: {e}")
            # Continue anyway as table might already be selected
            
    def setup_page(self) -> None:
        """Complete initial page setup."""
        self.accept_cookies()
        self.select_stockholm_tab()
        self.select_table_view()
        
    def extract_current_date(self) -> str:
        """Extract the currently selected date from the date picker."""
        try:
            # Try multiple possible ID patterns
            possible_ids = ["Agsid-15", "Agsid-8", "Agsid-1"]
            
            for elem_id in possible_ids:
                try:
                    date_input = self.driver.find_element(By.ID, elem_id)
                    if date_input:
                        return date_input.get_attribute("value")
                except:
                    continue
                    
            # Fallback: find by type and readonly attributes
            date_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='text'][readonly]")
            for input_elem in date_inputs:
                value = input_elem.get_attribute("value")
                if value and "-" in value:  # Looks like a date
                    return value
                    
        except Exception as e:
            self.logger.error(f"Error extracting date: {e}")
            
        return None
        
    def extract_table_data(self) -> Tuple[pd.DataFrame, str]:
        """
        Extract table data for the current date.
        
        Returns:
            Tuple of (DataFrame with table data, current date string)
        """
        try:
            # Wait for table to be present
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.table--striped"))
            )
            time.sleep(2)  # Allow table to fully render
            
            # Get current date
            current_date = self.extract_current_date()
            if not current_date:
                self.logger.warning("Could not extract date, using today's date")
                current_date = datetime.now().strftime('%Y-%m-%d')
            
            # Find and parse table
            table = self.driver.find_element(By.CSS_SELECTOR, "table.table--striped")
            rows = table.find_elements(By.TAG_NAME, "tr")
            
            if not rows:
                raise ValueError("No rows found in table")
            
            # Extract headers
            headers = []
            header_cells = rows[0].find_elements(By.TAG_NAME, "th")
            for th in header_cells:
                headers.append(th.text.strip())
            
            if not headers:
                raise ValueError("No headers found in table")
            
            # Extract data rows
            data = []
            for row in rows[1:]:
                cols = row.find_elements(By.TAG_NAME, "td")
                if cols:
                    values = []
                    for col in cols:
                        text = col.get_attribute("textContent") or col.text or ""
                        # Clean the text
                        text = text.replace('\xa0', '').replace('\u00a0', '')
                        text = text.replace(' ', '')  # Remove spaces in numbers
                        text = text.replace('.', '')  # Remove thousand separators
                        text = text.replace(',', '.')  # Swedish decimal to standard
                        text = text.strip()
                        values.append(text)
                    
                    if any(values):  # Skip empty rows
                        data.append(values)
            
            if not data:
                raise ValueError("No data rows found in table")
            
            # Create DataFrame
            df = pd.DataFrame(data, columns=headers)
            
            # Add date column
            df['Date'] = current_date
            
            # Convert numeric columns
            numeric_columns = ["Prognos (MW)", "Förbrukning (MW)"]
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].replace('-', ''), errors="coerce")
            
            # Create DateTime column if "Timme" exists
            if "Timme" in df.columns:
                df['DateTime'] = df.apply(
                    lambda row: f"{row['Date']} {row['Timme'].split('-')[0].strip()}", 
                    axis=1
                )
            
            self.logger.info(f"Extracted {len(df)} rows for date {current_date}")
            return df, current_date
            
        except Exception as e:
            self.logger.error(f"Error extracting table data: {e}")
            # Take screenshot for debugging if in GitHub Actions
            if os.environ.get('GITHUB_ACTIONS'):
                self.driver.save_screenshot("error_screenshot.png")
            raise
            
    def navigate_to_date_via_calendar(self, target_date: str) -> bool:
        """
        Navigate to a specific date using the calendar picker.
        
        Args:
            target_date: Date string in format 'YYYY-MM-DD'
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Navigating to date: {target_date}")
            
            # Parse the target date
            year, month, day = target_date.split('-')
            
            # Click the calendar icon to open date picker
            calendar_opened = False
            
            # Try clicking the calendar icon
            try:
                calendar_icon = self.driver.find_element(By.CSS_SELECTOR, ".date-time-picker .bi-calendar2-date")
                calendar_icon.click()
                calendar_opened = True
            except:
                # Try clicking the date input field
                try:
                    date_input = self.driver.find_element(By.CSS_SELECTOR, ".date-time-picker input[readonly]")
                    date_input.click()
                    calendar_opened = True
                except:
                    pass
                    
            if not calendar_opened:
                self.logger.error("Could not open calendar picker")
                return False
                
            time.sleep(1)
            
            # Navigate year
            try:
                current_year_elem = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".year-select .current-val"))
                )
                current_year = current_year_elem.text.strip()
                
                year_diff = int(year) - int(current_year)
                if year_diff < 0:
                    for _ in range(abs(year_diff)):
                        year_btn = self.driver.find_element(By.CSS_SELECTOR, ".year-select button:first-child")
                        if not year_btn.get_attribute('disabled'):
                            year_btn.click()
                            time.sleep(0.3)
            except Exception as e:
                self.logger.warning(f"Could not navigate year: {e}")
                
            # Navigate month
            month_names = {
                '01': 'Januari', '02': 'Februari', '03': 'Mars',
                '04': 'April', '05': 'Maj', '06': 'Juni',
                '07': 'Juli', '08': 'Augusti', '09': 'September',
                '10': 'Oktober', '11': 'November', '12': 'December'
            }
            
            target_month = month_names.get(month, 'Augusti')
            
            try:
                current_month_elem = self.driver.find_element(By.CSS_SELECTOR, ".month-select .current-val")
                current_month = current_month_elem.text.strip()
                
                months_order = list(month_names.values())
                if current_month in months_order and target_month in months_order:
                    current_idx = months_order.index(current_month)
                    target_idx = months_order.index(target_month)
                    month_diff = target_idx - current_idx
                    
                    if month_diff != 0:
                        selector = ".month-select button:first-child" if month_diff < 0 else ".month-select button:last-child"
                        for _ in range(abs(month_diff)):
                            month_btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                            if not month_btn.get_attribute('disabled'):
                                month_btn.click()
                                time.sleep(0.3)
            except Exception as e:
                self.logger.warning(f"Could not navigate month: {e}")
                
            # Click the day
            try:
                day_btn = self.driver.find_element(By.CSS_SELECTOR, f"button[data-date='{target_date}']")
                if not day_btn.get_attribute('disabled'):
                    day_btn.click()
                    time.sleep(0.5)
            except:
                self.logger.warning(f"Could not click specific day for {target_date}")
                
            # Confirm selection
            try:
                select_btn = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Välj')]")
                select_btn.click()
            except:
                try:
                    select_btn = self.driver.find_element(By.CSS_SELECTOR, "button[data-action='setNewDate']")
                    select_btn.click()
                except:
                    self.logger.warning("Could not find confirm button")
                    
            time.sleep(3)
            
            # Verify navigation
            new_date = self.extract_current_date()
            if new_date == target_date:
                self.logger.info(f"✓ Successfully navigated to {target_date}")
                return True
            else:
                self.logger.warning(f"Date navigation may have failed. Expected {target_date}, got {new_date}")
                return new_date == target_date
                
        except Exception as e:
            self.logger.error(f"Error in calendar navigation: {e}")
            return False
            
    def go_to_previous_day(self) -> bool:
        """
        Navigate to the previous day using the date picker button.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Store current date
            current_date = self.extract_current_date()
            
            # Find previous day button
            prev_button = None
            selectors = [
                ".graphPowerConsumption .date-time-picker button.button-left",
                ".date-time-picker button.button-left",
                "button[aria-label*='föregående dag']"
            ]
            
            for selector in selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        if elem.is_displayed() and elem.is_enabled():
                            prev_button = elem
                            break
                    if prev_button:
                        break
                except:
                    continue
                    
            if not prev_button:
                self.logger.error("Previous day button not found")
                return False
                
            # Click the button
            self.driver.execute_script("arguments[0].scrollIntoView(true);", prev_button)
            time.sleep(0.5)
            
            try:
                self.driver.execute_script("arguments[0].click();", prev_button)
            except:
                prev_button.click()
                
            time.sleep(3)
            
            # Verify date changed
            new_date = self.extract_current_date()
            if new_date != current_date:
                self.logger.info(f"→ Navigated from {current_date} to {new_date}")
                return True
            else:
                self.logger.warning("Date did not change after clicking previous button")
                return False
                
        except Exception as e:
            self.logger.error(f"Error navigating to previous day: {e}")
            return False
            
    def scrape_multiple_days(self, num_days: int = 7, start_date: Optional[str] = None) -> pd.DataFrame:
        """
        Scrape data for multiple days going backwards.
        
        Args:
            num_days: Number of days to scrape
            start_date: Optional start date in 'YYYY-MM-DD' format
            
        Returns:
            DataFrame with all collected data
        """
        all_data = []
        
        # Setup locale for Swedish if possible
        try:
            locale.setlocale(locale.LC_ALL, 'sv_SE.UTF-8')
        except:
            try:
                locale.setlocale(locale.LC_ALL, 'Swedish_Sweden.1252')
            except:
                self.logger.debug("Could not set Swedish locale")
        
        # Setup the page
        self.logger.info("Setting up page...")
        self.setup_page()
        
        # Navigate to start date if specified
        if start_date:
            if not self.navigate_to_date_via_calendar(start_date):
                self.logger.warning(f"Could not navigate to {start_date}, starting from current date")
        
        # Scrape data for each day
        for i in range(num_days):
            self.logger.info(f"Scraping day {i+1}/{num_days}...")
            
            try:
                # Extract data for current date
                df_day, date_str = self.extract_table_data()
                self.logger.info(f"  Date: {date_str}, Rows: {len(df_day)}")
                
                # Add to collection
                all_data.append(df_day)
                
                # Go to previous day if not the last iteration
                if i < num_days - 1:
                    if not self.go_to_previous_day():
                        self.logger.error("Could not navigate to previous day, stopping")
                        break
                        
            except Exception as e:
                self.logger.error(f"Error scraping day {i+1}: {e}")
                
                # Try to continue
                if i < num_days - 1:
                    try:
                        self.go_to_previous_day()
                    except:
                        break
        
        # Combine all dataframes
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            
            # Sort by DateTime if available
            if 'DateTime' in final_df.columns:
                try:
                    final_df['DateTime'] = pd.to_datetime(final_df['DateTime'])
                    final_df = final_df.sort_values('DateTime')
                except:
                    pass
                    
            self.logger.info(f"Total rows collected: {len(final_df)}")
            return final_df
        else:
            self.logger.warning("No data collected")
            return pd.DataFrame()