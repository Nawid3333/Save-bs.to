"""
BS.TO Series Scraper v2 - Config-based, clean implementation
Automatically scrapes watched TV series from bs.to with configurable selectors
"""

import time
import random
import json
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
import sys
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import threading
import atexit
import signal
import subprocess

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import USERNAME, PASSWORD, TIMEOUT, HEADLESS, DATA_DIR, SERIES_INDEX_FILE, SELECTORS_CONFIG

# Performance settings
# Allow overriding worker count via environment variable BS_MAX_WORKERS
MAX_WORKERS = int(os.getenv("BS_MAX_WORKERS", "6"))
USE_PARALLEL = True


def is_regular_season(season_label):
    """
    Check if season label is a regular numbered season (Staffel 1, Season 2, etc.)
    vs special content (Specials, OVA, Movies, Filme, etc.)
    
    Returns True for regular seasons that can use the 'assume unwatched' optimization.
    Returns False for special seasons that should always be checked individually.
    """
    import re
    # Match patterns like 'Staffel 1', 'Season 2', 'S1', just '1', '2', etc.
    return bool(re.search(r'^(staffel|season|s)?\s*\d+$', season_label.strip(), re.IGNORECASE))


def cleanup_firefox():
    """Kill only tracked geckodriver processes (Selenium-spawned Firefox only).
    
    IMPORTANT: This only kills geckodriver.exe by PID, NOT firefox.exe.
    Personal Firefox browser instances are never touched.
    """
    try:
        # Try to load tracked worker PIDs
        worker_pids_file = os.path.join(DATA_DIR, '.worker_pids.json')
        if os.path.exists(worker_pids_file):
            try:
                with open(worker_pids_file, 'r') as f:
                    pids = json.load(f)
                    for worker_id, pid in pids.items():
                        try:
                            if sys.platform == 'win32':
                                os.system(f'taskkill /F /PID {pid} /T 2>nul')
                            else:
                                os.system(f'kill -9 {pid} 2>/dev/null')
                        except Exception:
                            pass
            except Exception:
                pass
        
        # Fallback: kill all geckodriver processes (safer than firefox.exe)
        if sys.platform == 'win32':
            os.system('taskkill /F /IM geckodriver.exe /T 2>nul')
        else:
            os.system('pkill -9 geckodriver 2>/dev/null')
    except Exception as e:
        pass


# Register cleanup on exit
atexit.register(cleanup_firefox)


class BsToScraper:
    """Config-based web scraper for BS.TO series"""
    
    def __init__(self):
        self.driver = None
        self.series_data = []
        self.config = SELECTORS_CONFIG
        self.auth_cookies = []
        self.checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')
        self.failed_file = os.path.join(DATA_DIR, '.failed_series.json')
        self.pause_file = os.path.join(DATA_DIR, '.scrape_pause')
        self.worker_pids_file = os.path.join(DATA_DIR, '.worker_pids.json')
        self.completed_links = set()
        self.failed_links = []
        self.worker_pids = {}  # {worker_id: geckodriver_pid}
        
        if not self.config:
            raise Exception("selectors_config.json not loaded. Check config.py")
    
    # ==================== MERGE HELPERS ====================
    
    def merge_series_entry(self, old_entry, new_entry):
        """Merge new series data into existing entry.
        
        Note: This merge does NOT preserve watched status - it uses the raw data
        from the website. Protection against unwatching is handled later in
        confirm_and_save_changes() where the user can choose to allow it.
        """
        old_entry['status'] = 'active'  # Mark as active again if was unavailable
        old_seasons = {s.get('season'): s for s in old_entry.get('seasons', [])}
        
        for new_season in new_entry.get('seasons', []):
            season_label = new_season.get('season')
            if season_label in old_seasons:
                # Keep existing episodes, add new ones from new_season
                old_eps = {ep.get('number'): ep for ep in old_seasons[season_label].get('episodes', [])}
                for new_ep in new_season.get('episodes', []):
                    ep_num = new_ep.get('number')
                    # Use the new watched status from website (no protection here)
                    old_eps[ep_num] = new_ep
                old_seasons[season_label]['episodes'] = list(old_eps.values())
            else:
                old_seasons[season_label] = new_season
        
        old_entry['seasons'] = list(old_seasons.values())
        # Recalculate watched count from merged episodes
        old_entry['watched_episodes'] = sum(
            sum(1 for ep in s.get('episodes', []) if ep.get('watched'))
            for s in old_entry['seasons']
        )
        old_entry['total_episodes'] = sum(s.get('total_episodes', 0) for s in old_entry['seasons'])
        old_entry['url'] = new_entry.get('url', old_entry.get('url'))
        
        return old_entry
    
    # ==================== CONFIG HELPERS ====================
    
    def get_selector(self, path):
        """Get selector from config using dot notation (e.g., 'login.username_field')"""
        keys = path.split('.')
        value = self.config.get('selectors', {})
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value
    
    def get_timing(self, key):
        """Get timing delay from config"""
        return self.config.get('timing', {}).get(key, 0.3)
    
    def get_site_url(self):
        """Get site URL from config"""
        return self.config.get('site_url', 'https://bs.to')
    
    def is_logged_in(self, driver):
        """Check if driver is logged in by looking for verification markers.
        
        Returns True if logged in, False otherwise.
        """
        try:
            page_html = driver.page_source.lower()
            login_config = self.get_selector('login')
            markers = login_config.get('verification_markers', ['logout', 'hallo']) if login_config else ['logout']
            markers_to_check = markers + [USERNAME.lower()]
            return any(token in page_html for token in markers_to_check)
        except Exception:
            return False
    
    def get_login_page(self):
        """Get login page URL from config"""
        return self.config.get('login_page', 'https://bs.to/login')
    
    # ==================== ELEMENT FINDING ====================
    
    def convert_selector_to_by(self, selector_type):
        """Convert config selector type to Selenium By"""
        by_map = {
            'id': By.ID,
            'name': By.NAME,
            'css': By.CSS_SELECTOR,
            'xpath': By.XPATH,
            'tag': By.TAG_NAME,
            'class': By.CLASS_NAME
        }
        return by_map.get(selector_type, By.CSS_SELECTOR)
    
    def find_element_from_config(self, driver, config_selectors, timeout=2):
        """Try to find element using list of selectors from config"""
        if not isinstance(config_selectors, list):
            config_selectors = [config_selectors]
        
        for selector_config in config_selectors:
            selector_type = selector_config.get('type', 'css')
            selector_value = selector_config.get('value')
            
            by = self.convert_selector_to_by(selector_type)
            
            try:
                element = WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((by, selector_value))
                )
                return element
            except:
                continue
        
        return None
    
    def wait_for_element(self, driver, selector_by, selector_value, timeout=None):
        """Wait for element to be present and visible"""
        if timeout is None:
            timeout = self.get_timing('timeout')
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((selector_by, selector_value))
            )
            return True
        except:
            return False
    
    def wait_for_css_element(self, driver, css_selector, timeout=None):
        """Wait for CSS selector element to be present"""
        return self.wait_for_element(driver, By.CSS_SELECTOR, css_selector, timeout)
    
    # ==================== CHECKPOINT SYSTEM ====================
    
    def save_checkpoint(self):
        """Save scraping checkpoint to resume later"""
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(list(self.completed_links), f, ensure_ascii=False)
        except Exception:
            pass
    
    def load_checkpoint(self):
        """Load checkpoint to resume from previous run"""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_links = set(data or [])
                    return True
        except Exception:
            pass
        return False
    
    def clear_checkpoint(self):
        """Clear checkpoint after successful completion"""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
        except Exception:
            pass
    
    def save_failed_series(self):
        """Save failed series links for later retry"""
        if not self.failed_links:
            return
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            with open(self.failed_file, 'w', encoding='utf-8') as f:
                json.dump(self.failed_links, f, ensure_ascii=False)
        except Exception:
            pass
    
    def load_failed_series(self):
        """Load previously failed series for retry"""
        try:
            if os.path.exists(self.failed_file):
                with open(self.failed_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data or []
        except Exception:
            pass
        return []
    
    def clear_failed_series(self):
        """Clear failed series list after successful retry"""
        try:
            if os.path.exists(self.failed_file):
                os.remove(self.failed_file)
        except Exception:
            pass
    
    def is_pause_requested(self):
        """Check if pause has been requested via pause file"""
        return os.path.exists(self.pause_file)
    
    def clear_pause_request(self):
        """Clear pause request file"""
        try:
            if os.path.exists(self.pause_file):
                os.remove(self.pause_file)
        except Exception:
            pass
    
    def save_worker_pid(self, worker_id, pid):
        """Track a worker's geckodriver PID"""
        self.worker_pids[str(worker_id)] = pid
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            with open(self.worker_pids_file, 'w', encoding='utf-8') as f:
                json.dump(self.worker_pids, f)
        except Exception:
            pass
    
    def clear_worker_pids(self):
        """Clear tracked worker PIDs after scraping"""
        self.worker_pids = {}
        try:
            if os.path.exists(self.worker_pids_file):
                os.remove(self.worker_pids_file)
        except Exception:
            pass
    
    # ==================== DRIVER SETUP ====================
    
    def setup_driver(self):
        """Initialize the Selenium WebDriver"""
        firefox_options = Options()
        if HEADLESS:
            firefox_options.add_argument("--headless")
        firefox_options.add_argument("--disable-gpu")
        # Performance/stealth preferences
        firefox_options.set_preference("permissions.default.image", 1)
        firefox_options.set_preference("media.autoplay.default", 1)
        firefox_options.set_preference("dom.ipc.processPrelaunch.enabled", False)
        firefox_options.set_preference("network.http.speculative-parallel-limit", 0)
        firefox_options.set_preference(
            "general.useragent.override",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )
        
        firefox_service = FirefoxService()
        self.driver = webdriver.Firefox(service=firefox_service, options=firefox_options)
        
        # Track main driver PID for sequential mode (option 9)
        try:
            if hasattr(firefox_service, 'process') and firefox_service.process:
                self.save_worker_pid(0, firefox_service.process.pid)  # worker_id=0 for main driver
        except Exception:
            pass
    
    def close(self):
        """Close the browser and clean up Firefox processes"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            print("✓ Browser closed")
        cleanup_firefox()
    
    # ==================== AUTHENTICATION ====================
    
    def login(self):
        """Login to bs.to using config-based selectors"""
        try:
            login_config = self.get_selector('login')
            if not login_config:
                raise Exception("Login config not found in selectors_config.json")
            
            login_page = self.get_login_page()
            print(f"→ Navigating to login page: {login_page}")
            self.driver.get(login_page)
            
            # Wait for login form to appear
            self.wait_for_css_element(self.driver, "input[type='submit'], button[type='submit']", timeout=3)

            # Username field
            username_field = self.find_element_from_config(
                self.driver, 
                login_config.get('username_field', []),
                timeout=3
            )
            if not username_field:
                raise Exception("Username field not found")
            username_field.send_keys(USERNAME)

            # Password field
            password_field = self.find_element_from_config(
                self.driver,
                login_config.get('password_field', []),
                timeout=3
            )
            if not password_field:
                raise Exception("Password field not found")
            password_field.send_keys(PASSWORD)

            # Submit button
            submit_button = self.find_element_from_config(
                self.driver,
                login_config.get('submit_button', []),
                timeout=2
            )

            print("→ Submitting login...")
            if submit_button:
                submit_button.click()
            else:
                password_field.send_keys("\n")

            # Wait for redirect or page load
            try:
                WebDriverWait(self.driver, TIMEOUT).until(
                    lambda d: d.current_url != login_page
                )
            except Exception:
                pass

            # Wait for body to fully load
            self.wait_for_css_element(self.driver, "body", timeout=3)
            
            # Verify login with markers from config
            if self.is_logged_in(self.driver):
                print("✓ Login completed")
                try:
                    self.auth_cookies = self.driver.get_cookies()
                except Exception:
                    self.auth_cookies = []
                return

            raise Exception(f"Login verification failed. URL: {self.driver.current_url}")

        except Exception as e:
            print(f"✗ Login failed: {str(e)}")
            raise
    
    # ==================== SERIES DISCOVERY ====================
    
    def get_all_series(self):
        """Get list of all series from andere-serien page"""
        try:
            print("→ Fetching list of all series...")
            
            series_config = self.get_selector('series_list')
            if not series_config:
                raise Exception("Series list config not found")
            
            site_url = self.get_site_url()
            series_page = series_config.get('page_url', '/andere-serien')
            all_series_url = f"{site_url}{series_page}"
            
            self.driver.get(all_series_url)
            time.sleep(self.get_timing('page_load_delay'))
            
            page_content = self.driver.page_source
            soup = BeautifulSoup(page_content, 'html.parser')
            
            series_list = []
            filter_terms = series_config.get('filter_descriptions', [])
            
            # Find all links and filter for series
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')
                if href.startswith('/serie/') or href.startswith('serie/'):
                    title = link.get_text(strip=True)
                    
                    # Filter out descriptions
                    if any(term in title for term in filter_terms):
                        continue
                    
                    if title and title.strip():
                        # Ensure proper URL construction
                        if not href.startswith('/'):
                            href = '/' + href
                        series_list.append({
                            'title': title,
                            'link': href,
                            'url': f"{site_url}{href}"
                        })
            
            # Remove duplicates while preserving order
            utility_pages = {'alle serien', 'andere serien', 'beliebte serien'}
            seen = set()
            unique_series = []
            for s in series_list:
                # Skip utility/navigation pages (case-insensitive)
                if s['title'].lower().strip() in utility_pages:
                    continue
                if s['link'] not in seen:
                    seen.add(s['link'])
                    unique_series.append(s)
            
            print(f"✓ Found {len(unique_series)} unique series")
            return unique_series
            
        except Exception as e:
            print(f"✗ Failed to get series list: {str(e)}")
            raise
    
    # ==================== SEASON SCRAPING ====================
    
    def get_season_links(self, html, base_url):
        """Extract season links from season selector (includes specials/season 0)
        
        Returns list of tuples: (label, href, watched_status)
        watched_status: 'full' = all watched (green), 'none' = unwatched (grey), 'partial' = needs loading
        """
        soup = BeautifulSoup(html, 'html.parser')
        season_links = []

        series_config = self.get_selector('series_page')
        if not series_config:
            return []
        
        seasons_selector = series_config.get('seasons_selector', {})
        selector_id = seasons_selector.get('value', 'seasons')
        
        # Get watched class config
        watched_class_config = series_config.get('season_watched_class', {})
        full_watched_class = watched_class_config.get('full', 'watched')
        
        seasons_div = soup.find(id=selector_id)
        if seasons_div:
            season_links_config = series_config.get('season_links', {})
            season_type = season_links_config.get('type', 'css')
            season_value = season_links_config.get('value')
            
            if season_type == 'css':
                season_elems = seasons_div.select(season_value)
            else:
                season_elems = seasons_div.find_all('a')
            
            for a in season_elems:
                label = a.get_text(strip=True)
                href = a.get('href', '')
                
                if not href.startswith('http'):
                    site_url = self.get_site_url()
                    href = f"{site_url}/{href.lstrip('/')}"
                
                # Detect watched status from parent <li> element's CSS class
                # Structure: li.watched > a (the watched class is on the li, not the a)
                parent_li = a.find_parent('li')
                if parent_li:
                    classes = parent_li.get('class', [])
                else:
                    classes = []
                if isinstance(classes, str):
                    classes = classes.split()
                
                if full_watched_class and full_watched_class in classes:
                    watched_status = 'full'  # All episodes watched (green) - skip loading
                else:
                    # No 'watched' class on parent li - needs loading to check episodes
                    watched_status = 'none'
                
                season_links.append((label, href, watched_status))

        # Deduplicate while preserving order
        seen = set()
        unique = []
        for label, href, watched_status in season_links:
            key = (label, href)
            if key not in seen:
                seen.add(key)
                unique.append((label, href, watched_status))
        
        return unique
    
    # ==================== EPISODE SCRAPING ====================
    
    def scrape_episodes_from_html(self, html):
        """Parse episodes table and detect watched status"""
        soup = BeautifulSoup(html, 'html.parser')
        episodes = []
        
        episode_config = self.get_selector('episodes')
        if not episode_config:
            return episodes
        
        table_config = episode_config.get('table', {})
        table_type = table_config.get('type', 'css')
        table_value = table_config.get('value')
        
        if table_type == 'css':
            table = soup.select_one(table_value)
        else:
            table = soup.find(table_value)
        
        if table:
            row_config = episode_config.get('table_rows', {})
            row_type = row_config.get('type', 'css')
            row_value = row_config.get('value')
            
            rows = table.select(row_value) if row_type == 'css' else table.find_all('tr')
            
            watched_indicator = episode_config.get('watched_indicator', {})
            indicator_type = watched_indicator.get('type', 'row_class')
            indicator_value = watched_indicator.get('value', 'watched')
            
            ep_num_cell = episode_config.get('episode_number_cell', 0)
            ep_title_cell = episode_config.get('episode_title_cell', 1)
            ep_title_selector = episode_config.get('episode_title_selector', 'strong')
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) > max(ep_num_cell, ep_title_cell):
                    ep_num = cols[ep_num_cell].get_text(strip=True)
                    
                    title_col = cols[ep_title_cell]
                    title_tag = title_col.find(ep_title_selector)
                    title = title_tag.get_text(strip=True) if title_tag else ''
                    
                    # Detect watched status
                    watched = False
                    if indicator_type == 'row_class':
                        row_classes = row.get('class', [])
                        if isinstance(row_classes, str):
                            row_classes = row_classes.split()
                        watched = indicator_value in row_classes
                    
                    episodes.append({
                        'number': ep_num,
                        'title': title,
                        'watched': watched
                    })
        
        return episodes
    
    # ==================== SERIES PROCESSING ====================
    
    def process_series_page(self, url, series_hint=None, existing_entry=None, parallel_mode=False):
        """Navigate to series page, extract title and all seasons/episodes
        
        Optimization: Uses cached episode data when season watched status matches:
        - Green (fully watched) + cached data → reuse cached episodes, mark all watched
        - Grey (unwatched) + cached data → reuse cached episodes, mark all unwatched
        
        Parallel mode optimization:
        - If Season 1 is grey AND all its episodes are unwatched, assume all subsequent
          grey seasons are also unwatched (saves page loads for completely unwatched series)
        """
        try:
            self.driver.get(url)
            # Wait for title to appear
            self.wait_for_css_element(self.driver, "h2", timeout=3)

            page_content = self.driver.page_source

            # Extract title
            title_info = self.extract_series_title(page_content)
            title_value = title_info if title_info else None

            # Get season links (including specials) with watched status
            season_links = self.get_season_links(page_content, url)
            if not season_links:
                season_links = [("1", url, "none")]  # Default: needs loading

            # Build lookup for existing seasons by label
            existing_seasons = {}
            if existing_entry and existing_entry.get('seasons'):
                for s in existing_entry['seasons']:
                    existing_seasons[s.get('season')] = s

            seasons_data = []
            total_watched = 0
            total_eps = 0
            skipped_seasons = 0
            
            # Parallel mode optimization: track if series is completely unwatched
            # If Season 1 is grey and all eps unwatched, assume rest of grey seasons are too
            assume_grey_unwatched = False

            for idx, season_item in enumerate(season_links):
                # Handle both old format (label, url) and new format (label, url, status)
                if len(season_item) == 3:
                    season_label, season_url, watched_status = season_item
                else:
                    season_label, season_url = season_item
                    watched_status = "none"  # Unknown, needs loading
                
                try:
                    cached_season = existing_seasons.get(season_label)
                    
                    # Parallel mode: if we've confirmed series is unwatched, skip grey REGULAR seasons
                    # Special seasons (OVA, Movies, Specials) are always checked individually
                    if parallel_mode and assume_grey_unwatched and watched_status == 'none' and is_regular_season(season_label):
                        if cached_season and cached_season.get('episodes'):
                            # Use cached episodes, mark all unwatched
                            episodes = []
                            for ep in cached_season['episodes']:
                                episodes.append({
                                    'number': ep.get('number', ''),
                                    'title': ep.get('title', ''),
                                    'watched': False
                                })
                            watched_count = 0
                            total_count = len(episodes)
                            skipped_seasons += 1
                        else:
                            # No cache but we assume unwatched - still need to load for episode list
                            self.driver.get(season_url)
                            self.wait_for_css_element(self.driver, "table.episodes", timeout=3)
                            season_html = self.driver.page_source
                            episodes = self.scrape_episodes_from_html(season_html)
                            # Force all unwatched (we know series is unwatched)
                            for ep in episodes:
                                ep['watched'] = False
                            watched_count = 0
                            total_count = len(episodes)
                    # Optimization: use cached data if available and status is definitive
                    elif cached_season and cached_season.get('episodes'):
                        cached_eps = cached_season['episodes']
                        
                        if watched_status == 'full':
                            # Season is fully watched (green) - use cached episodes, mark all watched
                            episodes = []
                            for ep in cached_eps:
                                episodes.append({
                                    'number': ep.get('number', ''),
                                    'title': ep.get('title', ''),
                                    'watched': True  # Override to watched
                                })
                            watched_count = len(episodes)
                            total_count = len(episodes)
                            skipped_seasons += 1
                        elif watched_status == 'none' and cached_season.get('watched_episodes', 0) == 0:
                            # Season is unwatched (grey) AND was unwatched before - use cached
                            episodes = []
                            for ep in cached_eps:
                                episodes.append({
                                    'number': ep.get('number', ''),
                                    'title': ep.get('title', ''),
                                    'watched': False  # Keep unwatched
                                })
                            watched_count = 0
                            total_count = len(episodes)
                            skipped_seasons += 1
                        else:
                            # Status unclear or partial - must load to check
                            self.driver.get(season_url)
                            self.wait_for_css_element(self.driver, "table.episodes", timeout=3)
                            season_html = self.driver.page_source
                            episodes = self.scrape_episodes_from_html(season_html)
                            watched_count = sum(1 for ep in episodes if ep['watched'])
                            total_count = len(episodes)
                    else:
                        # No cached data - must load
                        self.driver.get(season_url)
                        self.wait_for_css_element(self.driver, "table.episodes", timeout=3)
                        season_html = self.driver.page_source
                        episodes = self.scrape_episodes_from_html(season_html)
                        watched_count = sum(1 for ep in episodes if ep['watched'])
                        total_count = len(episodes)
                    
                    # Parallel mode: after first grey season, check if all unwatched
                    # If so, we can assume all subsequent grey seasons are unwatched too
                    if parallel_mode and idx == 0 and watched_status == 'none' and watched_count == 0 and total_count > 0:
                        assume_grey_unwatched = True

                    seasons_data.append({
                        "season": season_label,
                        "url": season_url,
                        "episodes": episodes,
                        "watched_episodes": watched_count,
                        "total_episodes": total_count
                    })

                    total_watched += watched_count
                    total_eps += total_count
                except Exception as e:
                    continue

            # Derive link
            link = series_hint.get('link') if series_hint else None
            if not link:
                from urllib.parse import urlparse
                parsed = urlparse(url)
                link = parsed.path or url
            if not link.startswith('/'):
                link = '/' + link

            return {
                "title": title_value or (series_hint.get('title') if series_hint else url),
                "link": link,
                "url": url,
                "seasons": seasons_data,
                "watched_episodes": total_watched,
                "total_episodes": total_eps,
                "skipped_seasons": skipped_seasons  # For stats
            }
        except Exception as e:
            raise Exception(f"Series processing failed for {url}: {str(e)}")
    
    def extract_series_title(self, html):
        """Extract series title from HTML using config selectors (no approval prompts)"""
        soup = BeautifulSoup(html, 'html.parser')
        
        series_config = self.get_selector('series_page')
        if not series_config:
            return None
        
        title_config = series_config.get('title', {})
        title_type = title_config.get('type', 'tag')
        title_value = title_config.get('value', 'h2')
        
        try:
            if title_type == 'tag':
                element = soup.find(title_value)
            else:
                element = soup.select_one(title_value)
            
            if element:
                # Extract main text, optionally removing small tags
                main_text = element.get_text(strip=True)
                if element.small:
                    small_text = element.small.get_text(strip=True)
                    main_text = main_text.replace(small_text, "").strip()
                
                return main_text if main_text else None
        except Exception:
            pass
        
        return None
    
    # ==================== SCRAPING MODES ====================
    
    def scrape_series_list(self):
        """Scrape all TV series"""
        try:
            all_series = self.get_all_series()

            if USE_PARALLEL:
                print("→ Starting series scraping (parallel mode)...")
                # Full scrape uses the configured pool (default MAX_WORKERS)
                self._scrape_series_parallel(all_series)
            else:
                print("→ Starting series scraping (sequential mode)...")
                self._scrape_series_sequential(all_series)

            print(f"\n✓ Successfully scraped {len(self.series_data)} series")

        except Exception as e:
            print(f"✗ Series scraping failed: {str(e)}")
            raise
    
    def _scrape_series_sequential(self, all_series):
        """Sequential scraping with progress bar and ETA"""
        start_time = time.time()
        try:
            for idx, series in enumerate(all_series, 1):
                try:
                    # Calculate progress and ETA
                    elapsed = time.time() - start_time
                    avg_time_per_series = elapsed / idx
                    remaining_series = len(all_series) - idx
                    eta_seconds = avg_time_per_series * remaining_series
                    eta_mins = int(eta_seconds / 60)
                    progress_pct = int((idx / len(all_series)) * 100)
                    
                    # Progress bar
                    bar_length = 30
                    filled = int(bar_length * idx / len(all_series))
                    bar = '█' * filled + '░' * (bar_length - filled)
                    
                    print(f"\n[{idx}/{len(all_series)}] [{bar}] {progress_pct}% | ETA: {eta_mins}m | {series['title']}")
                    
                    result = self.process_series_page(series['url'], series_hint=series)
                    if result:
                        # Mark empty series
                        if result['total_episodes'] == 0:
                            result['empty'] = True
                            print(f"  ⚠ {result['title']}: No episodes (marked as empty)")
                        else:
                            result['empty'] = False
                            print(f"  ✓ {result['title']}: {result['watched_episodes']}/{result['total_episodes']} watched")
                        self.series_data.append(result)
                        # Save checkpoint
                        self.completed_links.add(series.get('link'))
                        self.save_checkpoint()
                    else:
                        print("  ⚠ Skipped (no data)")
                except Exception as e:
                    print(f"  ⚠ Error processing {series['title']}: {str(e)}")
                    continue
        except KeyboardInterrupt:
            print("\n\n⚠ Scraping interrupted by user (Ctrl+C)")
            print(f"✓ Progress saved: {len(self.series_data)}/{len(all_series)} series scraped")
            print("→ Use 'Resume from checkpoint' option to continue later\n")
            return
        
        # Final summary
        total_time = time.time() - start_time
        total_mins = int(total_time / 60)
        total_secs = int(total_time % 60)
        print(f"\n✓ Completed in {total_mins}m {total_secs}s", flush=True)
    
    def _scrape_series_parallel(self, all_series, worker_cap=None):
        """Parallel scraping with pre-partitioned work pools. Each worker gets its own dedicated list."""
        # Clear any leftover pause file from previous runs
        self.clear_pause_request()
        
        self.series_data = []
        start_time = time.time()
        completed = 0
        failed = 0
        skipped_seasons_total = 0
        lock = threading.Lock()
        auth_lock = threading.Lock()  # Serialize worker authentication
        stop_event = threading.Event()

        # Load existing index for season skip optimization
        existing_index = {}
        try:
            if os.path.exists(SERIES_INDEX_FILE):
                with open(SERIES_INDEX_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                for item in data or []:
                    title = item.get('title')
                    if title:
                        existing_index[title] = item
            if existing_index:
                print(f"→ Loaded {len(existing_index)} cached series for season skip optimization")
        except Exception as e:
            print(f"  ⚠ Could not load existing index: {e}")

        # Filter utility pages (case-insensitive)
        utility_pages = {'alle serien', 'andere serien', 'beliebte serien'}
        filtered_series = [s for s in all_series if s.get('title', '').lower().strip() not in utility_pages]
        total_series = len(filtered_series)

        max_workers_allowed = worker_cap if worker_cap is not None else MAX_WORKERS
        worker_count = min(max_workers_allowed, total_series) or 1
        
        # Pre-partition work: divide series evenly among workers
        worker_pools = [[] for _ in range(worker_count)]
        for idx, series in enumerate(filtered_series):
            worker_pools[idx % worker_count].append(series)
        
        pool_sizes = [len(p) for p in worker_pools]
        print(f"→ Pre-partitioned {total_series} series across {worker_count} workers ({min(pool_sizes)}-{max(pool_sizes)} each)")

        def progress_line(done, total, title, watched=None, episode_total=None, empty=False, error=None, worker_id=None):
            elapsed = time.time() - start_time
            avg = elapsed / max(1, done + failed)
            remaining = total - (done + failed)
            eta_mins = int((avg * remaining) / 60)
            pct = int(((done + failed) / total) * 100)
            bar_len = 30
            filled = int(bar_len * (done + failed) / total)
            bar = '█' * filled + '░' * (bar_len - filled)
            worker_info = f" | W{worker_id}/{worker_count}" if worker_id else f" | Workers: {worker_count}"
            ep_info = f" ({watched}/{episode_total})" if watched is not None and episode_total is not None else ""
            if error:
                print(f"[{done+failed}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m{worker_info} | ✗ {title}: {error}")
            elif empty:
                print(f"[{done+failed}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m{worker_info} | ⚠ {title}: No episodes")
            else:
                print(f"[{done+failed}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m{worker_info} | ✓ {title}{ep_info}")

        def worker_loop(worker_id, my_series_list):
            nonlocal completed, failed, skipped_seasons_total
            driver = self._create_worker_driver(worker_id)
            success_delay = self.get_timing('success_delay') or 0.2
            backoff_base = self.get_timing('error_backoff_base') or 0.5
            backoff_max = self.get_timing('error_backoff_max') or 5.0
            health_every_val = self.get_timing('health_check_every') or 10
            restart_threshold_val = self.get_timing('error_restart_threshold') or 5
            try:
                health_every = int(health_every_val) if health_every_val and health_every_val >= 1 else 10
            except Exception:
                health_every = 10
            try:
                restart_threshold = int(restart_threshold_val) if restart_threshold_val and restart_threshold_val >= 2 else 5
            except Exception:
                restart_threshold = 5

            error_streak = 0
            tasks_since_check = 0
            
            # Authentication must be sequential across all workers to avoid rate limiting
            with auth_lock:
                # Each worker does its own full login (no cookie sharing - more reliable)
                authenticated = False
                max_auth_retries = 3
                for attempt in range(max_auth_retries):
                    try:
                        self._login_with_driver(driver)
                        
                        # Verify login worked
                        driver.get(self.get_site_url())
                        time.sleep(1.0)
                        
                        if self.is_logged_in(driver):
                            authenticated = True
                            break
                        else:
                            print(f"  ⚠ Worker #{worker_id}: Login verification failed (try {attempt + 1}/{max_auth_retries})")
                            time.sleep(1)
                    except Exception as e:
                        print(f"  ⚠ Worker #{worker_id}: Login failed - {str(e)[:80]}")
                        time.sleep(1)
                        continue
            
            if not authenticated:
                print(f"  ✗ Worker #{worker_id}: Failed to authenticate after {max_auth_retries} attempts. Aborting to prevent incorrect data.")
                return  # Exit worker without processing

            # Process dedicated series list (no queue competition)
            for series in my_series_list:
                if stop_event.is_set():
                    break
                    
                # Check for pause request
                if self.is_pause_requested():
                    print(f"\n⏸ Worker #{worker_id} pausing (pause file detected)")
                    break

                try:
                    # Pass existing entry for season skip optimization
                    existing_entry = existing_index.get(series.get('title'))
                    result = self._process_series_with_driver(driver, series['url'], series, existing_entry)
                    with lock:
                        if result:
                            skipped_seasons_total += result.get('skipped_seasons', 0)
                            empty = result.get('total_episodes', 0) == 0
                            result['empty'] = empty
                            self.series_data.append(result)
                            self.completed_links.add(series.get('link'))
                            completed += 1
                            if completed % 10 == 0:
                                self.save_checkpoint()
                            watched = result.get('watched_episodes', 0)
                            total_eps = result.get('total_episodes', 0)
                            progress_line(completed, total_series, result.get('title', 'Series'), watched=watched, episode_total=total_eps, empty=empty, worker_id=worker_id)
                            error_streak = 0
                            tasks_since_check += 1
                        else:
                            failed += 1
                            progress_line(completed, total_series, series.get('title', 'Series'), error='skipped', worker_id=worker_id)
                            error_streak += 1
                except Exception as e:
                    with lock:
                        failed += 1
                        self.failed_links.append(series)
                    progress_line(completed, total_series, series.get('title', 'Series'), error=str(e)[:120], worker_id=worker_id)
                    error_streak += 1

                if error_streak == 0 and success_delay and success_delay > 0:
                    try:
                        time.sleep(min(1.0, float(success_delay)))
                    except Exception:
                        pass
                if error_streak > 0:
                    try:
                        exp = min(6, error_streak)
                        backoff = min(float(backoff_max), float(backoff_base) * (2 ** exp))
                        jitter = random.uniform(0, 0.3)
                        time.sleep(backoff + jitter)
                    except Exception:
                        time.sleep(0.5)

                do_health_check = (tasks_since_check >= health_every) or (error_streak >= 3)
                if do_health_check:
                    tasks_since_check = 0
                    try:
                        driver.get(self.get_site_url())
                        if not self.is_logged_in(driver):
                            try:
                                self._login_with_driver(driver)
                                error_streak = 0
                            except Exception:
                                error_streak += 1
                    except Exception:
                        error_streak += 1

                if error_streak >= restart_threshold:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = self._create_worker_driver(worker_id)
                    cookies_ok = self._apply_cookies_to_driver(driver)
                    if not cookies_ok:
                        try:
                            self._login_with_driver(driver)
                        except Exception:
                            pass
                    error_streak = 0

            try:
                driver.quit()
            except Exception:
                pass

        executor = ThreadPoolExecutor(max_workers=worker_count)
        futures = []
        try:
            for worker_id in range(1, worker_count + 1):
                worker_pool = worker_pools[worker_id - 1]
                print(f"  🔺 Worker #{worker_id} starting ({len(worker_pool)} series)")
                futures.append(executor.submit(worker_loop, worker_id, worker_pool))
                # Stagger worker startup to avoid simultaneous auth attempts
                time.sleep(1.0)

            # Wait for all workers to complete (not just queue to empty)
            for f in as_completed(futures):
                pass
        except KeyboardInterrupt:
            print("\n\n⚠ Scraping interrupted by user (Ctrl+C)")
            print(f"✓ Progress saved: {len(self.series_data)}/{total_series} series scraped")
            print("→ Use 'Resume from checkpoint' option to continue later\n")
            stop_event.set()
            executor.shutdown(wait=False, cancel_futures=True)
            return
        finally:
            stop_event.set()
            executor.shutdown(wait=True, cancel_futures=False)
            for f in futures:
                try:
                    f.result()
                except Exception:
                    pass

        # Check if pause was requested
        if self.is_pause_requested():
            print(f"\n⏸ Scraping paused by user")
            print(f"✓ Progress saved: {len(self.series_data)}/{total_series} series scraped")
            print(f"→ Resume later with checkpoint option or use 'Retry failed' for errors\n")
            self.clear_pause_request()
        
        # Save failed series for later retry
        if self.failed_links:
            self.save_failed_series()
            print(f"\n⚠ {len(self.failed_links)} series failed. Saved to .failed_series.json for retry.")

        total_time = time.time() - start_time
        total_mins = int(total_time / 60)
        total_secs = int(total_time % 60)
        skip_info = f" | {skipped_seasons_total} season pages skipped (cached)" if skipped_seasons_total > 0 else ""
        if failed:
            print(f"\n✓ Completed in {total_mins}m {total_secs}s ({failed} failed){skip_info}", flush=True)
        else:
            print(f"\n✓ Completed in {total_mins}m {total_secs}s{skip_info}", flush=True)
    
    def _scrape_series_worker(self, series):
        """Worker for parallel processing"""
        try:
            worker_driver = self._create_worker_driver()
            
            # Try cookie-based auth
            cookies_ok = self._apply_cookies_to_driver(worker_driver)
            if cookies_ok:
                worker_driver.get(self.get_site_url())
                if not self.is_logged_in(worker_driver):
                    cookies_ok = False
            
            if not cookies_ok:
                self._login_with_driver(worker_driver)
            
            # Scrape the series
            result = self._process_series_with_driver(worker_driver, series['url'], series)
            worker_driver.quit()
            
            return result
        except Exception as e:
            try:
                worker_driver.quit()
            except:
                pass
            raise e
    
    def _create_worker_driver(self, worker_id=None):
        """Create a new WebDriver for worker thread and track its PID"""
        firefox_options = Options()
        if HEADLESS:
            firefox_options.add_argument('--headless')
        firefox_options.add_argument('--disable-blink-features=AutomationControlled')
        firefox_options.set_preference("permissions.default.image", 1)
        firefox_options.set_preference("media.autoplay.default", 1)
        firefox_options.set_preference("dom.ipc.processPrelaunch.enabled", False)
        firefox_options.set_preference("network.http.speculative-parallel-limit", 0)
        firefox_options.set_preference(
            "general.useragent.override",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )
        
        gecko_path = os.path.join(os.path.dirname(__file__), '..', 'geckodriver.exe')
        if os.path.exists(gecko_path):
            service = FirefoxService(gecko_path)
        else:
            service = FirefoxService()
        
        driver = webdriver.Firefox(service=service, options=firefox_options)
        
        # Track geckodriver PID from service if available (with retry)
        if worker_id is not None:
            pid_saved = False
            for attempt in range(3):
                try:
                    if hasattr(service, 'process') and service.process:
                        pid = service.process.pid
                        if pid:
                            self.save_worker_pid(worker_id, pid)
                            pid_saved = True
                            break
                except Exception:
                    pass
                time.sleep(0.1)  # Brief delay for service to initialize
            
            if not pid_saved:
                print(f"  ⚠ Worker #{worker_id}: Could not track PID")
        
        return driver
    
    def _apply_cookies_to_driver(self, driver):
        """Apply auth cookies to worker driver"""
        if not self.auth_cookies:
            return False
        try:
            driver.get(self.get_site_url())
            time.sleep(self.get_timing('action_delay'))
            for cookie in self.auth_cookies:
                try:
                    driver.add_cookie({
                        'name': cookie.get('name'),
                        'value': cookie.get('value'),
                        'domain': cookie.get('domain'),
                        'path': cookie.get('path', '/'),
                        'secure': cookie.get('secure', False),
                        'httpOnly': cookie.get('httpOnly', False)
                    })
                except Exception:
                    continue
            driver.get(self.get_site_url())
            return True
        except Exception:
            return False
    
    def _login_with_driver(self, driver):
        """Login using worker driver"""
        try:
            login_page = self.get_login_page()
            driver.get(login_page)
            # Wait for login form
            self.wait_for_css_element(driver, "input[type='submit'], button[type='submit']", timeout=3)

            login_config = self.get_selector('login')
            if not login_config:
                raise Exception("Login config not found")
            
            username_field = self.find_element_from_config(
                driver,
                login_config.get('username_field', []),
                timeout=3
            )
            if not username_field:
                raise Exception("Username field not found")
            username_field.send_keys(USERNAME)

            password_field = self.find_element_from_config(
                driver,
                login_config.get('password_field', []),
                timeout=3
            )
            if not password_field:
                raise Exception("Password field not found")
            password_field.send_keys(PASSWORD)

            submit_button = self.find_element_from_config(
                driver,
                login_config.get('submit_button', []),
                timeout=2
            )

            if submit_button:
                submit_button.click()
            else:
                password_field.send_keys("\n")

            # Wait for body to load
            self.wait_for_css_element(driver, "body", timeout=3)
            
            if not self.is_logged_in(driver):
                raise Exception("Worker login verification failed")
        except Exception as e:
            raise Exception(f"Worker login failed: {str(e)}")
    
    def _process_series_with_driver(self, driver, url, series_hint=None, existing_entry=None):
        """Process series using specific driver (used by parallel workers)
        
        Optimization: Uses cached episode data when season watched status matches:
        - Green (fully watched) + cached data → reuse cached episodes, mark all watched
        - Grey (unwatched) + cached data with 0 watched → reuse cached, keep unwatched
        
        Parallel mode optimization:
        - If Season 1 is grey AND all its episodes are unwatched, assume all subsequent
          grey seasons are also unwatched (saves page loads for completely unwatched series)
        """
        try:
            driver.get(url)
            # Wait for title to appear
            self.wait_for_css_element(driver, "h2", timeout=3)
            page_content = driver.page_source
            
            title_info = self.extract_series_title(page_content)
            title_value = title_info if title_info else None
            
            season_links = self.get_season_links(page_content, url)
            if not season_links:
                season_links = [("1", url, "none")]
            
            # Build lookup for existing seasons by label
            existing_seasons = {}
            if existing_entry and existing_entry.get('seasons'):
                for s in existing_entry['seasons']:
                    existing_seasons[s.get('season')] = s
            
            seasons_data = []
            total_watched = 0
            total_eps = 0
            skipped_seasons = 0
            
            # Parallel mode optimization: track if series is completely unwatched
            # If Season 1 is grey and all eps unwatched, assume rest of grey seasons are too
            assume_grey_unwatched = False
            
            for idx, season_item in enumerate(season_links):
                # Handle both old format (label, url) and new format (label, url, status)
                if len(season_item) == 3:
                    season_label, season_url, watched_status = season_item
                else:
                    season_label, season_url = season_item
                    watched_status = "none"
                
                try:
                    cached_season = existing_seasons.get(season_label)
                    
                    # Parallel optimization: if we've confirmed series is unwatched, skip grey REGULAR seasons
                    # Special seasons (OVA, Movies, Specials) are always checked individually
                    if assume_grey_unwatched and watched_status == 'none' and is_regular_season(season_label):
                        if cached_season and cached_season.get('episodes'):
                            # Use cached episodes, mark all unwatched
                            episodes = []
                            for ep in cached_season['episodes']:
                                episodes.append({
                                    'number': ep.get('number', ''),
                                    'title': ep.get('title', ''),
                                    'watched': False
                                })
                            watched_count = 0
                            total_count = len(episodes)
                            skipped_seasons += 1
                        else:
                            # No cache but we assume unwatched - still need to load for episode list
                            driver.get(season_url)
                            self.wait_for_css_element(driver, "table.episodes", timeout=3)
                            season_html = driver.page_source
                            episodes = self.scrape_episodes_from_html(season_html)
                            # Force all unwatched (we know series is unwatched)
                            for ep in episodes:
                                ep['watched'] = False
                            watched_count = 0
                            total_count = len(episodes)
                    # Optimization: use cached data if available and status is definitive
                    elif cached_season and cached_season.get('episodes'):
                        cached_eps = cached_season['episodes']
                        
                        if watched_status == 'full':
                            # Season is fully watched (green) - use cached episodes, mark all watched
                            episodes = []
                            for ep in cached_eps:
                                episodes.append({
                                    'number': ep.get('number', ''),
                                    'title': ep.get('title', ''),
                                    'watched': True
                                })
                            watched_count = len(episodes)
                            total_count = len(episodes)
                            skipped_seasons += 1
                        elif watched_status == 'none' and cached_season.get('watched_episodes', 0) == 0:
                            # Season is unwatched (grey) AND was unwatched before - use cached
                            episodes = []
                            for ep in cached_eps:
                                episodes.append({
                                    'number': ep.get('number', ''),
                                    'title': ep.get('title', ''),
                                    'watched': False
                                })
                            watched_count = 0
                            total_count = len(episodes)
                            skipped_seasons += 1
                        else:
                            # Status unclear or partial - must load to check
                            driver.get(season_url)
                            self.wait_for_css_element(driver, "table.episodes", timeout=3)
                            season_html = driver.page_source
                            episodes = self.scrape_episodes_from_html(season_html)
                            watched_count = sum(1 for ep in episodes if ep['watched'])
                            total_count = len(episodes)
                    else:
                        # No cached data - must load
                        driver.get(season_url)
                        self.wait_for_css_element(driver, "table.episodes", timeout=3)
                        season_html = driver.page_source
                        episodes = self.scrape_episodes_from_html(season_html)
                        watched_count = sum(1 for ep in episodes if ep['watched'])
                        total_count = len(episodes)
                    
                    # After first grey season, check if all unwatched
                    # If so, we can assume all subsequent grey seasons are unwatched too
                    if idx == 0 and watched_status == 'none' and watched_count == 0 and total_count > 0:
                        assume_grey_unwatched = True
                    
                    seasons_data.append({
                        "season": season_label,
                        "url": season_url,
                        "episodes": episodes,
                        "watched_episodes": watched_count,
                        "total_episodes": total_count
                    })
                    
                    total_watched += watched_count
                    total_eps += total_count
                except Exception as e:
                    continue
            
            from urllib.parse import urlparse
            link = series_hint.get('link') if series_hint else None
            if not link:
                parsed = urlparse(url)
                link = parsed.path or url
            if not link.startswith('/'):
                link = '/' + link
            
            return {
                "title": title_value or (series_hint.get('title') if series_hint else url),
                "link": link,
                "url": url,
                "seasons": seasons_data,
                "watched_episodes": total_watched,
                "total_episodes": total_eps,
                "skipped_seasons": skipped_seasons
            }
        except Exception as e:
            raise Exception(f"Series processing failed for {url}: {str(e)}")
    
    # ==================== DATA MANAGEMENT ====================
    
    def load_existing_links(self):
        """Load existing series links to avoid re-scraping"""
        existing = set()
        try:
            if os.path.exists(SERIES_INDEX_FILE):
                with open(SERIES_INDEX_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                for item in data or []:
                    link = item.get('link')
                    if link:
                        existing.add(link)
        except Exception:
            pass
        return existing
    
    def scrape_new_series_only(self):
        """Scrape only new series not in index"""
        all_series = self.get_all_series()
        existing_links = self.load_existing_links()
        new_series_list = [s for s in all_series if s.get('link') not in existing_links]

        print(f"→ New series to scrape: {len(new_series_list)} (out of {len(all_series)})")
        self.series_data = []
        if not new_series_list:
            print("✓ No new series detected — skipping scraper spin-up")
            return

        # Run sequential to avoid extra workers for the small delta set
        self._scrape_series_sequential(new_series_list)
    
    def scrape_resume_checkpoint(self):
        """Resume from checkpoint"""
        if not self.load_checkpoint():
            print("⚠ No checkpoint found. Starting fresh...")
            self.scrape_series_list()
            return
        
        all_series = self.get_all_series()
        remaining_series = [s for s in all_series if s.get('link') not in self.completed_links]
        
        completed_count = len(all_series) - len(remaining_series)
        print(f"✓ Resuming from checkpoint: {completed_count}/{len(all_series)} already done")
        print(f"→ Remaining to scrape: {len(remaining_series)}")
        
        self.series_data = []
        if not remaining_series:
            print("✓ All series already scraped")
            return

        if USE_PARALLEL:
            # Resume can use full pool; series count will bound actual worker usage
            self._scrape_series_parallel(remaining_series)
        else:
            self._scrape_series_sequential(remaining_series)
    
    def scrape_retry_failed(self, output_file):
        """Retry previously failed series"""
        failed_list = self.load_failed_series()
        if not failed_list:
            print("✓ No failed series found")
            return
        
        print(f"✓ Found {len(failed_list)} failed series from last run")
        self.series_data = []
        
        # Run sequential for reliability on retry
        self._scrape_series_sequential(failed_list)
    
    # ==================== MAIN RUN METHODS ====================
    
    def scrape_single_series(self, url):
        """Scrape exactly one series (sequential, accurate)"""
        print(f"→ Scraping single series: {url}")
        result = self.process_series_page(url)
        self.series_data = [result]
        return result
    
    def scrape_multiple_series(self, urls):
        """Scrape multiple series from a list of URLs (sequential, accurate)"""
        print(f"→ Scraping {len(urls)} series from URL list (sequential mode)...")
        # Convert URLs to series-like dicts for reuse of _scrape_series_sequential
        series_list = [{'title': url.split('/')[-1], 'link': url, 'url': url} for url in urls]
        self.series_data = []
        self._scrape_series_sequential(series_list)
        print(f"  Successfully scraped: {len(self.series_data)}/{len(urls)} series")
    
    def run(self, output_file, single_url=None, url_list=None, new_only=False, resume_only=False, retry_failed=False, parallel=None):
        """Execute scraping workflow - collects data but does NOT save (caller handles save)"""
        global USE_PARALLEL
        original_use_parallel = USE_PARALLEL
        
        if parallel is not None:
            USE_PARALLEL = parallel
            mode_str = "parallel" if parallel else "sequential"
            print(f"→ Using {mode_str} mode")
        
        try:
            self.setup_driver()
            self.login()
            
            if single_url:
                self.scrape_single_series(single_url)
            elif url_list:
                self.scrape_multiple_series(url_list)
            else:
                if retry_failed:
                    print("→ Running in 'retry failed series' mode")
                    self.scrape_retry_failed(output_file)
                elif resume_only:
                    print("→ Running in 'resume from checkpoint' mode")
                    self.scrape_resume_checkpoint()
                elif new_only:
                    print("→ Running in 'new series only' mode")
                    self.scrape_new_series_only()
                else:
                    self.scrape_series_list()
            
            self.clear_checkpoint()
            self.clear_failed_series()
            self.clear_worker_pids()
        finally:
            USE_PARALLEL = original_use_parallel
            self.close()
