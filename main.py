#!/usr/bin/env python3
"""
BS.TO Series Scraper and Index Manager
Automatically scrapes your watched TV series from bs.to and maintains a local index
"""

import json
import sys
import os

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
    print("  5. Show series with progress (sorted)")
    print("  6. Batch add series from text file")
    print("  7. Retry failed series from last run")
    print("  8. Pause current scraping (in another terminal)")
    print("  9. Show active workers")
    print("  10. Exit\n")


def scrape_series():
    """Execute series scraping with optional resume from checkpoint"""
    print("\n→ Starting BS.TO scraper...")
    print("  (Browser will open - do not close it manually)\n")
    
    # Check if checkpoint exists
    checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')
    resume = False
    if os.path.exists(checkpoint_file):
        print("⚠ Checkpoint found from previous run!\n")
        choice = input("Resume from checkpoint? (y/n): ").strip().lower()
        resume = choice == 'y'
    
    # Ask for scraping mode
    print("\nScraping mode:")
    print("  1. Sequential (slower, but most reliable)")
    print("  2. Parallel (faster, uses multiple workers)\n")
    mode_choice = input("Choose mode (1-2) [default: 2]: ").strip() or '2'
    use_parallel = mode_choice == '2'
    
    try:
        scraper = BsToScraper()
        scraper.run(SERIES_INDEX_FILE, resume_only=resume, parallel=use_parallel)
        
        # Use scraped data directly from scraper and confirm before saving
        if scraper.series_data:
            if confirm_and_save_changes(scraper.series_data, "Scraped data"):
                print("\n✓ Scraping completed successfully!")
        else:
            print("\n⚠ No data scraped")
        
    except Exception as e:
        print(f"\n✗ Scraping failed: {str(e)}")


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
        else:
            print("\n⚠ No new series found")
    except Exception as e:
        print(f"\n✗ New-only scraping failed: {str(e)}")


def create_progress_bar(percent, width=20):
    """Create a visual progress bar"""
    filled = int(width * percent / 100)
    bar = "█" * filled + "░" * (width - filled)
    return bar


def show_series_with_progress():
    """Display all series with progress bars and sorting options"""
    manager = IndexManager()
    
    print("\n📊 SERIES PROGRESS VIEW")
    print("-" * 60)
    print("\nSort by:")
    print("  1. Completion % (incomplete first)")
    print("  2. Series name (A-Z)")
    print("  3. Date added")
    print("  4. Total episodes (least first)")
    print("  5. Total episodes (most first)")
    
    sort_choice = input("\nChoose sort option (1-5) [default: 1]: ").strip() or '1'
    compact_choice = input("Compact view? (y/n) [default: y]: ").strip().lower() or 'y'
    compact = compact_choice != 'n'
    
    print("\nFilter:")
    print("  1. All series")
    print("  2. Only unwatched (0% complete)")
    print("  3. Only unfinished (started but incomplete)")
    
    filter_choice = input("\nChoose filter (1-3) [default: 1]: ").strip() or '1'
    
    sort_map = {
        '1': 'completion',
        '2': 'name',
        '3': 'date',
        '4': 'total_episodes',
        '5': 'total_episodes_desc'
    }
    sort_by = sort_map.get(sort_choice, 'completion')
    
    # Get series with progress
    series_list = manager.get_series_with_progress(sort_by=sort_by)
    
    # Apply filters
    if filter_choice == '2':
        series_list = [s for s in series_list if s['watched_episodes'] == 0]
    elif filter_choice == '3':
        series_list = [s for s in series_list if s['is_incomplete'] and s['watched_episodes'] > 0]
    
    if not series_list:
        print("✗ No series found\n")
        return
    
    page_size = 25

    title_width = 32 if compact else 40
    bar_width = 14 if compact else 20
    status_width = 9 if compact else 10
    separator = "-" * (title_width + bar_width + status_width + 8)

    total_items = len(series_list)
    index = 0
    page_num = 1
    while index < total_items:
        end = min(index + page_size, total_items)
        print(f"\nPage {page_num} ({index+1}-{end} of {total_items})")
        print(f"{'Série':<{title_width}} {'Progress':<{bar_width+6}} {'Status':>{status_width}}")
        print(separator)
        
        for series in series_list[index:end]:
            title = series['title'][:title_width-2]  # Truncate long names
            progress_bar = create_progress_bar(series['completion'], width=bar_width)
            percent = series['completion']
            watched = series['watched_episodes']
            total = series['total_episodes']
            
            # Status indicator
            if series['is_incomplete']:
                status = f"⚠️ {percent}%"
                watched_info = f"({watched}/{total})"
            else:
                status = f"✓ 100%"
                watched_info = f"({watched}/{total})"
            
            print(f"{title:<{title_width}} {progress_bar} {watched_info:>7}  {status:>{status_width}}")
        
        print(separator)
        index = end
        page_num += 1
        if index < total_items:
            cont = input("More? (Enter = next, q = stop): ").strip().lower()
            if cont == 'q':
                break
    
    print(f"\nTotal: {total_items} series")
    incomplete_series = [s for s in series_list if s['is_incomplete']]
    incomplete = len(incomplete_series)
    if incomplete > 0:
        print(f"⚠️  {incomplete} series incomplete")
        
        # Offer to rescrape all incomplete
        rescrape = input(f"\nRescrape all {incomplete} incomplete series for updates? (y/n): ").strip().lower()
        if rescrape == 'y':
            # Build URL list from incomplete series
            urls = []
            for s in incomplete_series:
                series_data = manager.series_index.get(s['title'], {})
                url = series_data.get('url') or series_data.get('link')
                if url:
                    if not url.startswith('http'):
                        url = f"https://bs.to{url}"
                    urls.append(url)
            
            if urls:
                print(f"\n→ Rescraping {len(urls)} incomplete series...")
                print("  (Browser will open - do not close it manually)\n")
                
                try:
                    scraper = BsToScraper()
                    scraper.run(SERIES_INDEX_FILE, url_list=urls)
                    
                    if scraper.series_data:
                        if confirm_and_save_changes(scraper.series_data, f"Rescrape data ({len(urls)} series)"):
                            print(f"\n✓ Rescrape completed! {len(urls)} series updated.")
                    else:
                        print("\n⚠ No data scraped")
                except Exception as e:
                    print(f"\n✗ Rescrape failed: {str(e)}")
            else:
                print("✗ Could not find URLs for incomplete series")
    else:
        print("✓ All series completed!\n")


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
                    else:
                        print("\n⚠ Could not extract URLs from ongoing series")
                except Exception as e:
                    print(f"\n✗ Failed to export URLs: {str(e)}")
        
    except Exception as e:
        print(f"\n✗ Failed to generate report: {str(e)}")


def add_series_by_url():
    """Add a single series to the index by pasting its URL"""
    print("\n→ Add single series by URL")
    print("  Example: https://bs.to/serie/Breaking-Bad\n")
    url = input("Enter series URL: ").strip()
    
    if not url:
        print("✗ No URL provided")
        return
    
    if not url.startswith("http"):
        print("✗ Invalid URL (must start with http:// or https://)")
        return
    
    print("\n→ Starting scraper for single series...")
    print("  (Browser will open - do not close it manually)\n")
    
    try:
        scraper = BsToScraper()
        scraper.run(SERIES_INDEX_FILE, single_url=url)
        
        # Use scraped data directly from scraper and confirm before saving
        if scraper.series_data:
            if confirm_and_save_changes(scraper.series_data, "Series data"):
                print("\n✓ Series added/updated successfully!")
        else:
            print("\n⚠ No data scraped")
    except Exception as e:
        print(f"\n✗ Failed to add series: {str(e)}")


def batch_add_series_from_file():
    """Add multiple series from a text file containing URLs (one per line)"""
    print("\n→ Batch add series from text file")
    print("  The file should contain one URL per line")
    print("  Example format:")
    print("    https://bs.to/serie/Breaking-Bad")
    print("    https://bs.to/serie/Game-of-Thrones")
    print("    https://bs.to/serie/The-Wire\n")
    
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
        else:
            print("\n⚠ No data scraped")
    except Exception as e:
        print(f"\n✗ Batch add failed: {str(e)}")


def retry_failed_series():
    """Retry previously failed series"""
    print("\n→ Retry failed series from last run")
    print("  (Browser will open - do not close it manually)\n")
    
    # Pre-check for failed series before launching browser
    from src.scraper import BsToScraper
    scraper = BsToScraper()
    failed_list = scraper.load_failed_series()
    if not failed_list:
        print("✓ No failed series found. Nothing to retry.")
        return
    print(f"✓ Found {len(failed_list)} failed series from last run")
    try:
        scraper.run(SERIES_INDEX_FILE, retry_failed=True)
        # Use scraped data directly from scraper and confirm before saving
        if scraper.series_data:
            if confirm_and_save_changes(scraper.series_data, "Retry data"):
                print("\n✓ Retry completed successfully!")
        else:
            print("\n⚠ No data to retry")
    except Exception as e:
        print(f"\n✗ Retry failed: {str(e)}")


def pause_scraping():
    """Create a pause file to signal workers to pause scraping"""
    pause_file = os.path.join(DATA_DIR, '.pause_scraping')
    try:
        with open(pause_file, 'w', encoding='utf-8') as f:
            f.write('PAUSE')
        print(f"\n✓ Pause file created: {pause_file}\nWorkers will pause at next checkpoint.")
    except Exception as e:
        print(f"\n✗ Failed to create pause file: {str(e)}")

def show_active_workers():
    """Display active worker processes"""
    worker_pids_file = os.path.join(os.path.dirname(__file__), 'data', '.worker_pids.json')
    
    if not os.path.exists(worker_pids_file):
        print("\n✓ No active workers found\n")
        return
    
    try:
        with open(worker_pids_file, 'r', encoding='utf-8') as f:
            workers = json.load(f)
        
        if not workers:
            print("\n✓ No active workers\n")
            return
        
        print(f"\n📊 ACTIVE WORKERS ({len(workers)}):")
        print("ID | PID | Type")
        print("---|-----|------")
        for worker_id, pid in sorted(workers.items(), key=lambda x: int(x[0])):
            worker_type = "Main" if worker_id == "0" else f"Worker"
            print(f"{worker_id:>2} | {pid} | {worker_type}")
        print()
        
        # Option to kill all workers
        kill_choice = input("Kill all workers? (y/n): ").strip().lower()
        if kill_choice == 'y':
            print("\n🔴 Killing all workers...")
            import subprocess
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
                except Exception:
                    pass
            
            # Clean up the PID file
            try:
                os.remove(worker_pids_file)
                print(f"✓ Killed {killed_count} worker(s) and cleaned up tracking file\n")
            except Exception:
                print(f"✓ Killed {killed_count} worker(s)\n")
        else:
            print("✓ Workers left running\n")
    except Exception as e:
        print(f"\n✗ Error reading workers: {str(e)}\n")


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
        
        if choice == '1':
            scrape_series()
        elif choice == '2':
            scrape_new_series()
        elif choice == '3':
            add_series_by_url()
        elif choice == '4':
            generate_report()
        elif choice == '5':
            show_series_with_progress()
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
        else:
            print("✗ Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
