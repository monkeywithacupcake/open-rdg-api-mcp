#!/usr/bin/env python3
"""
USDA Rural Data Gateway Data Fetcher
Automates the multi-step download process for CSV data
"""

from playwright.sync_api import sync_playwright
import time
from pathlib import Path

class USDADataFetcher:
    def __init__(self, download_dir="./data"):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)

    def _perform_download(self, url: str, filename_prefix: str) -> Path:
        """
        Download CSV data from the RDG
        Handles: Export to CSV -> popup -> Download button -> new tab -> accept download
        """
        with sync_playwright() as p:
            print("Launching browser...")
            browser = None
            try:
                browser = p.firefox.launch(headless=False)
                print("Browser launched successfully")
                
                context = browser.new_context(
                    accept_downloads=True,
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )                
                page = context.new_page()
                
                try:
                    print(f"Navigating to {url}")
                    page.goto(url, wait_until="networkidle", timeout=60000)
                    
                    print("Waiting for page to load completely...")
                    page.wait_for_timeout(5000)  # Wait 5 seconds cuz tableau is slooow
                    
                    print("Looking for Export to CSV button...")
                    export_selectors = [
                        'button:has-text("Export to CSV")',  # Text-based fallback
                    ]
                    
                    export_button = None
                    for selector in export_selectors:
                        try:
                            export_button = page.wait_for_selector(selector, timeout=5000)
                            if export_button:
                                print(f"Found export element with selector: {selector}")
                                break
                        except Exception as e:
                            print(f"Selector {selector} failed: {e}")
                            continue
                    
                    if not export_button:
                        print("Could not find the export button")
                        return None
                    
                    export_button.click()
                    print("Clicked export button")
                    
                    # Wait a moment for the download crosstab popup/modal to appear
                    page.wait_for_timeout(2000)


                    with page.expect_download() as download_info:
                      with page.expect_popup() as page1_info:
                        iframe_locator = page.frame_locator('iframe[title="Data Visualization"]')
                        the_download_button = iframe_locator.locator("[data-test-id=\"DownloadLink\"]")
                        the_download_button.click()
                        download = download_info.value
                        filename = f"{filename_prefix}_{int(time.time())}.csv"
                        file_path = self.download_dir / filename
                        print(f"Saving {filename_prefix} download as {filename}")
                        download.save_as(str(file_path))
                            
                    print(f"Successfully downloaded {filename_prefix}: {file_path}")
                    return file_path
                    
                except Exception as e:
                    print(f"Error during {filename_prefix} download: {e}")
                    print(f"Error type: {type(e).__name__}")
                    return None
                
                finally:
                    if browser:
                        print(f"Closing browser for {filename_prefix} download...")
                        try:
                            browser.close()
                        except Exception as e:
                            print(f"Error closing browser: {e}")
                            
            except Exception as e:
                print(f"Error setting up browser: {e}")
                print(f"Error type: {type(e).__name__}")
                if browser:
                    try:
                        browser.close()
                    except:
                        pass
                return None
            
    def download_detail_data(self) -> Path:
        detail_url = "https://www.rd.usda.gov/rural-data-gateway/rural-investments/data"
        print("Starting download of detailed transaction data...")
        return self._perform_download(detail_url, "usda_rural_detail")
    
    def download_summary_data(self) -> Path:
        summary_url = "https://www.rd.usda.gov/rural-data-gateway/rural-investments"
        print("Starting download of historical summary data...")
        return self._perform_download(summary_url, "usda_rural_hist")
    
    def download_both_datasets(self) -> dict:
       
        # Download detailed data first
        detail_file = self.download_detail_data()
        print("\nWaiting 3 seconds before next download...")
        time.sleep(3)
        
        # Download summary data
        summary_file = self.download_summary_data()
        results = {
            "detail": detail_file,
            "summary": summary_file
        }
        return results


if __name__ == "__main__":
    fetcher = USDADataFetcher()
    result = fetcher.download_both_datasets()
    
    if result:
        print(f"Download completed: {result}")
    else:
        print("Download failed")