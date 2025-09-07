#!/usr/bin/env python3
"""
USDA Rural Data Gateway Data Fetcher
Automates the multi-step download process for CSV data
"""

from playwright.sync_api import sync_playwright
import os
import time
from pathlib import Path

class USDADataFetcher:
    def __init__(self, download_dir="./data"):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        
    def download_filtered_data(self, url="https://www.rd.usda.gov/rural-data-gateway/rural-investments/data"):
        """
        Download CSV data from the Data Download Page
        this page only has current year 20205
        will need to test with main page
        Handles: Export to CSV -> popup -> Download button -> new tab -> accept download
        """
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
                    
                    # Wait a moment for the popup/modal to appear
                    page.wait_for_timeout(2000)

                    # Try different selectors for the download button in the dialog
                    download_selectors = [
                        '[class*="suppressClickBusting"]', # srsly
                        "button:has-text('Download')",  # do not click Data Download
                        '.tabDownloadFileButton',  # The actual Tableau download link
                        'a.tabDownloadFileButton',  # More specific - anchor with class
                        '[class*="tabDownloadFileButton"]',  # Partial class match
                    ]
                    
                    download_button = None
                    for selector in download_selectors:
                        try:
                            download_button = page.wait_for_selector(selector, timeout=5000)
                            print(f"Found download button with selector: {selector}")
                            break
                        except:
                            continue
                    
                    if not download_button:
                        print("No download button found. Maybe direct download? Waiting for download...")
                        # if user hits the button manually, will work
                        try:
                            with page.expect_download(timeout=10000) as download_info:
                                pass  # Download may have already started
                            
                            download = download_info.value
                            filename = f"usda_rural_data_{int(time.time())}.csv"
                            file_path = self.download_dir / filename
                            
                            print(f"Saving download as {filename}")
                            download.save_as(str(file_path))
                            
                        except Exception as e:
                            print(f"No direct download either: {e}")
                            return None
                    else:
                        # Found download button, click it
                        print("Setting up download handler...")
                        try:
                            with page.expect_download(timeout=30000) as download_info:
                                download_button.click()
                                print("Clicked download button")
                            
                            download = download_info.value
                            filename = f"usda_rural_data_{int(time.time())}.csv"
                            file_path = self.download_dir / filename
                            
                            print(f"Saving download as {filename}")
                            download.save_as(str(file_path))
                            
                        except Exception as e:
                            print(f"Download failed: {e}")
                            return None
                    
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