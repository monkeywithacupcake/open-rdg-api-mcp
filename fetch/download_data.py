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
        
    def download_rdg_data(self):
        """
        Download CSV data from the RDG
        Gets both data from overview and details
        ONLY GETS details for current year, if get all, it times out
        Handles: Export to CSV -> popup -> Download button -> new tab -> accept download
        """
        #hist_url = "https://www.rd.usda.gov/rural-data-gateway/rural-investments"
        detail_url = "https://www.rd.usda.gov/rural-data-gateway/rural-investments/data"

        with sync_playwright() as p:
            print("Launching browser...")
            browser = None
            try:
                browser = p.firefox.launch(
                    headless=False
                )
                print("Browser launched successfully")
                
                context = browser.new_context(
                    accept_downloads=True,
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )                
                page = context.new_page()
                
                try:
                    print(f"Navigating to {detail_url}")
                    page.goto(detail_url, wait_until="networkidle", timeout=60000)
                    
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
                                # Show what we found
                                text = export_button.inner_text()[:50] if export_button.inner_text() else "No text"
                                classes = export_button.get_attribute('class') or ''
                                print(f"  Element text: '{text}'")
                                print(f"  Element classes: '{classes}'")
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
                        filename = f"usda_rural_data_{int(time.time())}.csv"
                        file_path = self.download_dir / filename
                        print(f"Saving download as {filename}")
                        download.save_as(str(file_path))
                            
                    print(f"Successfully downloaded: {file_path}")
                    return file_path
                    
                except Exception as e:
                    print(f"Error during download: {e}")
                    print(f"Error type: {type(e).__name__}")
                    return None
                
                finally:
                    if browser:
                        print("Closing browser...")
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

if __name__ == "__main__":
    fetcher = USDADataFetcher()
    result = fetcher.download_filtered_data()
    if result:
        print(f"Download completed: {result}")
    else:
        print("Download failed")