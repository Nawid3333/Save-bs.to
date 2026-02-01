#!/usr/bin/env python3
"""
BS.TO Series Scraper and Index Manager
Automatically scrapes your watched TV series from bs.to and maintains a local index
"""

import json
import sys
import os
import logging
import re
import subprocess
from requests.exceptions import RequestException
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from config.config import SERIES_INDEX_FILE, USERNAME, PASSWORD, DATA_DIR
from src.scraper import BsToScraper
from src.index_manager import IndexManager, confirm_and_save_changes


def print_header():
    """Print application header"""
    print("\n" + "="*60)
    print("  BS.TO SERIES SCRAPER & INDEX MANAGER")
    print("="*60 + "\n")


def print_scraped_series_status():
    """Reload index and print status for all newly scraped/updated series"""
    try:
        index_manager = IndexManager()
        index_manager.load_index()
        
        if not index_manager.series_index:
            return
        
        # Show top updated/new series with their complete episode counts
        series_list = list(index_manager.series_index.values())
        if not series_list:
            return
        
        # Sort by last updated (most recent first)
        sorted_series = sorted(
            series_list,
            key=lambda s: s.get('last_updated', s.get('added_date', '')),
            reverse=True
        )
        
        # Show first 5 updated series
        display_count = min(5, len(sorted_series))
        if display_count > 0:
            print("\n" + "-"*70)
            print("EPISODE STATUS (from merged index):")
            print("-"*70)
            for s in sorted_series[:display_count]:
                watched = s.get('watched_episodes', 0)
                total = s.get('total_episodes', 0)
                percent = round((watched / total * 100), 1) if total else 0
                print(f"  • {s.get('title')}: {watched}/{total} episodes ({percent}%)")
    except Exception as e:
        logger.error(f"Error printing series status: {e}")


def validate_credentials():
    """Validate that credentials are configured"""
    if not USERNAME or not PASSWORD:
        print("✗ ERROR: Credentials not configured!")
        print("\nPlease follow these steps:")
        print("1. Copy '.env.example' to '.env'")
        print("2. Add your bs.to username and password to the .env file")
        print("3. Save the file and try again\n")
        return False
    return True


def show_menu():
    """Display interactive menu"""
    print("\nOptions:")
    print("  1. Scrape series from bs.to (requires login)")
    print("  2. Scrape only NEW series (faster)")
    print("  3. Add single series by URL")
    print("  4. Generate full report")
    print("  5. Export data to CSV")
    print("  6. Batch add series from text file")
    print("  7. Retry failed series from last run")
    print("  8. Pause current scraping (in another terminal)")
    print("  9. Show active workers")
    print(" 10. Exit\n")


def scrape_series():
    """Execute series scraping with optional resume from checkpoint"""
    print("\n→ Starting BS.TO scraper...")
    print("  (Browser will open - do not close it manually)\n")
    
    try:
        # Check if checkpoint exists
        checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')
        resume = False
        if os.path.exists(checkpoint_file):
            print("⚠ Checkpoint found from previous run!\n")
            choice = input("Resume from checkpoint? (y/n): ").strip().lower()
            resume = choice == 'y'
        
        # Validate user input for scraping mode
        print("\nScraping mode:")
        print("  1. Sequential (slower, but most reliable)")
        print("  2. Parallel (faster, uses multiple workers)\n")
        mode_choice = input("Choose mode (1-2) [default: 2]: ").strip() or '2'
        
        if mode_choice not in ['1', '2']:
            print("⚠ Invalid choice, using default (parallel)")
            use_parallel = True
        else:
            use_parallel = mode_choice == '2'
        
        scraper = BsToScraper()
        scraper.run(SERIES_INDEX_FILE, resume_only=resume, parallel=use_parallel)
        
        # Use scraped data directly from scraper and confirm before saving
        if scraper.series_data:
            if confirm_and_save_changes(scraper.series_data, "Scraped data"):
                print("\n✓ Scraping completed successfully!")
                print_scraped_series_status()
                logger.info("Scraping completed successfully")
        else:
            print("\n⚠ No data scraped")
            logger.warning("No data scraped during scraping operation")
        
    except RequestException as e:
        print(f"\n✗ Network error occurred: {str(e)}")
        logger.error(f"Network error in scrape_series: {e}")
    except KeyboardInterrupt:
        print("\n⚠ Scraping interrupted by user")
        logger.info("Scraping interrupted by user")
    except Exception as e:
        print(f"\n✗ Unexpected error during scraping: {str(e)}")
        logger.error(f"Unexpected error in scrape_series: {e}")


def scrape_new_series():
    """Execute scraping only for new series not yet in the index"""
    print("\n→ Starting BS.TO scraper (NEW series only)...")
    print("  (Browser will open - do not close it manually)\n")
    
    try:
        scraper = BsToScraper()
        scraper.run(SERIES_INDEX_FILE, new_only=True)

        # Use scraped data directly from scraper and confirm before saving
        if scraper.series_data:
            if confirm_and_save_changes(scraper.series_data, "New series data"):
                print("\n✓ New series scraping completed successfully!")
                print_scraped_series_status()
                logger.info("New series scraping completed successfully")
        else:
            print("\n⚠ No new series found")
            logger.warning("No new series found during scraping operation")
    except RequestException as e:
        print(f"\n✗ Network error occurred: {str(e)}")
        logger.error(f"Network error in scrape_new_series: {e}")
    except KeyboardInterrupt:
        print("\n⚠ Scraping interrupted by user")
        logger.info("New series scraping interrupted by user")
    except Exception as e:
        print(f"\n✗ Unexpected error during new-only scraping: {str(e)}")
        logger.error(f"Unexpected error in scrape_new_series: {e}")


def generate_report():
    """Generate and save full report"""
    manager = IndexManager()
    report = manager.get_full_report()
    
    report_file = os.path.join(os.path.dirname(__file__), 'data', 'series_report.json')
    
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\n✓ Report saved to: {report_file}")
        
        # Display summary
        meta = report['metadata']
        stats = meta['statistics']
        print(f"\n  Total series:       {stats['total_series']}")
        print(f"  Watched (100%):     {stats['watched']}")
        
        ongoing_count = len(report.get('ongoing', []))
        not_started_count = len(report.get('not_started', []))
        print(f"  Ongoing (started):  {ongoing_count}")
        print(f"  Not started:        {not_started_count}")
        print(f"  Generated:          {meta['generated']}")
        
        # Show ongoing series (started but incomplete)
        if ongoing_count > 0:
            print(f"\n📺 ONGOING SERIES ({ongoing_count}):")
            for title in report['ongoing'][:10]:
                print(f"  • {title}")
            if ongoing_count > 10:
                print(f"  ... and {ongoing_count - 10} more\n")
            
            # Offer to export ongoing series URLs to series_urls.txt
            export = input(f"\nExport {ongoing_count} ongoing series URLs to series_urls.txt? (y/n): ").strip().lower()
            if export == 'y':
                try:
                    # Get URLs for ongoing series from the index
                    urls = []
                    for title in report['ongoing']:
                        series_data = manager.series_index.get(title, {})
                        url = series_data.get('url') or series_data.get('link')
                        if url:
                            # Ensure full URL
                            if not url.startswith('http'):
                                url = f"https://bs.to{url}"
                            urls.append(url)
                    
                    if urls:
                        # Write to series_urls.txt
                        urls_file = os.path.join(os.path.dirname(__file__), 'series_urls.txt')
                        with open(urls_file, 'w', encoding='utf-8') as f:
                            f.write('\n'.join(urls) + '\n')
                        print(f"\n✓ Exported {len(urls)} URLs to series_urls.txt")
                        print(f"  → Use option 6 (Batch add) to rescrape these series")
                        logger.info(f"Exported {len(urls)} URLs to series_urls.txt")
                    else:
                        print("\n⚠ Could not extract URLs from ongoing series")
                        logger.warning("Could not extract URLs from ongoing series for export")
                except Exception as e:
                    print(f"\n✗ Failed to export URLs: {str(e)}")
                    logger.error(f"Failed to export URLs: {e}")
        
    except Exception as e:
        print(f"\n✗ Failed to generate report: {str(e)}")
        logger.error(f"Failed to generate report: {e}")


def add_series_by_url():
    """Add a single series to the index by pasting its URL"""
    print("\n→ Add single series by URL")
    print("  Example: https://bs.to/serie/Breaking-Bad\n")
    
    while True:
        url = input("Enter series URL: ").strip()
        # Validate URL format
        if not url:
            print("✗ No URL provided")
            continue
        if not url.startswith(("http://", "https://")):
            print("✗ Invalid URL (must start with http:// or https://)")
            continue
        try:
            parsed_url = urlparse(url)
            if not parsed_url.netloc or 'bs.to' not in parsed_url.netloc:
                print("✗ Invalid bs.to URL")
                continue
            # Require /serie/ in path
            if not re.search(r"/serie/[^/]+", parsed_url.path):
                print("✗ URL must be a valid bs.to series page (e.g. https://bs.to/serie/Breaking-Bad)")
                continue
        except Exception as e:
            print("✗ Invalid URL format")
            logger.error(f"Invalid URL format: {url}, error: {e}")
            continue
        break
    
    print("\n→ Starting scraper for single series...")
    print("  (Browser will open - do not close it manually)\n")
    
    try:
        scraper = BsToScraper()
        scraper.run(SERIES_INDEX_FILE, single_url=url)
        # Use scraped data directly from scraper and confirm before saving
        if scraper.series_data:
            if confirm_and_save_changes(scraper.series_data, "Series data"):
                # Print watched/total episodes for the updated series
                series = None
                # scraper.series_data may be a list or dict
                if isinstance(scraper.series_data, list):
                    for s in scraper.series_data:
                        if s.get('url') == url or s.get('link') == url:
                            series = s
                            break
                    if not series and len(scraper.series_data) == 1:
                        series = scraper.series_data[0]
                elif isinstance(scraper.series_data, dict):
                    # Try to find by url or just take the first
                    for s in scraper.series_data.values():
                        if s.get('url') == url or s.get('link') == url:
                            series = s
                            break
                    if not series:
                        series = next(iter(scraper.series_data.values()))

                # After saving, reload the series from the index to get complete merged data
                index_manager = IndexManager()
                index_manager.load_index()
                
                # Find the scraped series in the index by title or link
                series_in_index = None
                if series:
                    series_title = series.get('title')
                    series_link = series.get('link')
                    for indexed_series in index_manager.series_index.values():
                        if indexed_series.get('title') == series_title or indexed_series.get('link') == series_link:
                            series_in_index = indexed_series
                            break
                
                if series_in_index:
                    watched = series_in_index.get('watched_episodes', 0)
                    total = series_in_index.get('total_episodes', 0)
                    percent = round((watched / total * 100), 1) if total else 0
                    print(f"\nStatus for '{series_in_index.get('title', url)}': {watched}/{total} episodes watched ({percent}%)")
                elif series:
                    watched = series.get('watched_episodes', 0)
                    total = series.get('total_episodes', 0)
                    percent = round((watched / total * 100), 1) if total else 0
                    print(f"\nStatus for '{series.get('title', url)}': {watched}/{total} episodes watched ({percent}%)")
                
                print("\n✓ Series added/updated successfully!")
                logger.info(f"Successfully added/updated series: {url}")
        else:
            print("\n⚠ No data scraped")
            logger.warning(f"No data scraped for URL: {url}")
    except RequestException as e:
        print(f"\n✗ Network error occurred: {str(e)}")
        logger.error(f"Network error adding series from URL {url}: {e}")
    except KeyboardInterrupt:
        print("\n⚠ Series addition interrupted by user")
        logger.info(f"Series addition interrupted for URL: {url}")
    except Exception as e:
        print(f"\n✗ Failed to add series: {str(e)}")
        logger.error(f"Failed to add series from URL {url}: {e}")


def batch_add_series_from_file():
    """Add multiple series from a text file containing URLs (one per line)"""
    print("\n→ Batch add series from text file")
    print("  The file should contain one URL per line")
    print("  Example format:")
    print("    https://bs.to/serie/Breaking-Bad")
    
    default_file = os.path.join(os.path.dirname(__file__), 'series_urls.txt')
    file_path = input(f"Enter file path [default: series_urls.txt]: ").strip().strip('"\'')
    
    if not file_path:
        file_path = default_file
    
    if not os.path.exists(file_path):
        print(f"✗ File not found: {file_path}")
        return
    
    # Read URLs from file
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip() and line.strip().startswith('http')]
    except Exception as e:
        print(f"✗ Failed to read file: {str(e)}")
        logger.error(f"Failed to read file {file_path}: {e}")
        return
    
    if not urls:
        print("✗ No valid URLs found in file")
        return
    
    print(f"\n✓ Found {len(urls)} URL(s) in file")
    print("\nURLs to process:")
    for url in urls:
        print(f"  • {url}")
    
    confirm = input("\nProceed with batch add? (y/n): ").strip().lower()
    if confirm != 'y':
        print("✗ Cancelled")
        return
    
    print("\n→ Starting batch scraper...")
    print("  (Browser will open - do not close it manually)\n")
    
    try:
        scraper = BsToScraper()
        scraper.run(SERIES_INDEX_FILE, url_list=urls)
        
        # Use scraped data directly from scraper and confirm before saving
        if scraper.series_data:
            if confirm_and_save_changes(scraper.series_data, f"Batch data ({len(urls)} series)"):
                print(f"\n✓ Batch add completed! {len(urls)} series processed.")
                print_scraped_series_status()
                logger.info(f"Batch add completed: {len(urls)} series processed")
        else:
            print("\n⚠ No data scraped")
            logger.warning("No data scraped during batch processing")
    except RequestException as e:
        print(f"\n✗ Network error occurred: {str(e)}")
        logger.error(f"Network error in batch processing: {e}")
    except KeyboardInterrupt:
        print("\n⚠ Batch processing interrupted by user")
        logger.info("Batch processing interrupted by user")
    except Exception as e:
        print(f"\n✗ Batch add failed: {str(e)}")
        logger.error(f"Batch add failed: {e}")


def retry_failed_series():
    """Retry previously failed series"""
    print("\n→ Retry failed series from last run")
    print("  (Browser will open - do not close it manually)\n")
    
    # Pre-check for failed series before launching browser
    try:
        from src.scraper import BsToScraper
        scraper = BsToScraper()
        failed_list = scraper.load_failed_series()
        if not failed_list:
            print("✓ No failed series found. Nothing to retry.")
            return
        print(f"✓ Found {len(failed_list)} failed series from last run")
        
        scraper.run(SERIES_INDEX_FILE, retry_failed=True)
        # Use scraped data directly from scraper and confirm before saving
        if scraper.series_data:
            if confirm_and_save_changes(scraper.series_data, "Retry data"):
                print("\n✓ Retry completed successfully!")
                print_scraped_series_status()
                logger.info(f"Retry completed successfully for {len(failed_list)} series")
        else:
            print("\n⚠ No data to retry")
            logger.warning("No data to retry in retry_failed_series")
    except RequestException as e:
        print(f"\n✗ Network error occurred: {str(e)}")
        logger.error(f"Network error in retry_failed_series: {e}")
    except Exception as e:
        print(f"\n✗ Retry failed: {str(e)}")
        logger.error(f"Retry failed: {e}")


def pause_scraping():
    """Create a pause file to signal workers to pause scraping"""
    pause_file = os.path.join(DATA_DIR, '.pause_scraping')
    try:
        with open(pause_file, 'w', encoding='utf-8') as f:
            f.write('PAUSE')
        print(f"\n✓ Pause file created: {pause_file}\nWorkers will pause at next checkpoint.")
        logger.info(f"Pause file created: {pause_file}")
    except Exception as e:
        print(f"\n✗ Failed to create pause file: {str(e)}")
        logger.error(f"Failed to create pause file {pause_file}: {e}")

def show_active_workers():
    """Display active worker processes"""
    worker_pids_file = os.path.join(DATA_DIR, '.worker_pids.json')
    
    if not os.path.exists(worker_pids_file):
        print("\n✓ No active workers found\n")
        return
    
    try:
        with open(worker_pids_file, 'r', encoding='utf-8') as f:
            workers = json.load(f)
        if not isinstance(workers, dict) or not workers:
            print("\n✓ No active workers\n")
            return
        print(f"\n📊 ACTIVE WORKERS ({len(workers)}):")
        print("ID | PID | Type")
        print("---|-----|------")
        try:
            for worker_id, pid in sorted(workers.items(), key=lambda x: int(x[0])):
                worker_type = "Main" if worker_id == "0" else f"Worker"
                print(f"{worker_id:>2} | {pid} | {worker_type}")
        except Exception as e:
            print("✗ Error parsing worker PIDs. File may be corrupted.")
            logger.error(f"Error parsing worker PIDs: {e}")
            return
        print()
        # Option to kill all workers
        kill_choice = input("Kill all workers? (y/n): ").strip().lower()
        if kill_choice == 'y':
            print("\n🔴 Killing all workers...")
            killed_count = 0
            for worker_id, pid in workers.items():
                try:
                    if sys.platform == 'win32':
                        subprocess.run(['taskkill', '/F', '/PID', str(pid), '/T'], 
                                     capture_output=True, check=False)
                    else:
                        subprocess.run(['kill', '-9', str(pid)], 
                                     capture_output=True, check=False)
                    killed_count += 1
                except Exception as e:
                    logger.error(f"Failed to kill worker {worker_id} (PID {pid}): {e}")
            # Clean up the PID file
            try:
                os.remove(worker_pids_file)
                print(f"✓ Killed {killed_count} worker(s) and cleaned up tracking file\n")
                logger.info(f"Killed {killed_count} workers and cleaned up tracking file")
            except Exception as e:
                print(f"✓ Killed {killed_count} worker(s)\n")
                logger.error(f"Failed to clean up worker PIDs file: {e}")
        else:
            print("✓ Workers left running\n")
    except Exception as e:
        print(f"\n✗ Error reading workers: {str(e)}")
        logger.error(f"Error reading workers from file {worker_pids_file}: {e}")


def export_to_csv():
    """Export series data to CSV file"""
    try:
        index_manager = IndexManager()
        filepath = index_manager.export_to_csv()
        print(f"\n✓ Data exported successfully!")
        print(f"  File: {filepath}")
        
        # Ask if user wants to open the file
        choice = input("\nOpen file location? (y/n): ").strip().lower()
        if choice == 'y':
            try:
                if os.name == 'nt':  # Windows
                    os.startfile(os.path.dirname(filepath))
                else:  # Linux/Mac
                    subprocess.run(['xdg-open', os.path.dirname(filepath)])
            except Exception as e:
                print(f"Could not open file location: {e}")
        
    except Exception as e:
        print(f"✗ Error exporting to CSV: {str(e)}")


def main():
    """Main application loop"""
    print_header()
    
    # Validate credentials
    if not validate_credentials():
        sys.exit(1)
        
    print(f"✓ Credentials found for user: {USERNAME}\n")
    
    while True:
        show_menu()
        choice = input("Enter your choice (1-10): ").strip()
        if not choice.isdigit() or not (1 <= int(choice) <= 10):
            print("✗ Invalid choice. Please enter a number between 1 and 10.")
            continue
        if choice == '1':
            scrape_series()
        elif choice == '2':
            scrape_new_series()
        elif choice == '3':
            add_series_by_url()
        elif choice == '4':
            generate_report()
        elif choice == '5':
            export_to_csv()
        elif choice == '6':
            batch_add_series_from_file()
        elif choice == '7':
            retry_failed_series()
        elif choice == '8':
            pause_scraping()
        elif choice == '9':
            show_active_workers()
        elif choice == '10':
            print("\n✓ Goodbye!\n")
            break


if __name__ == "__main__":
    main()
