"""
BS.TO Series Scraper

Config-driven scraper using Selenium + BeautifulSoup.
Supports sequential and parallel (ThreadPoolExecutor) modes,
checkpoints, retry, and atomic JSON writes.
"""

import atexit
import json
import os
import queue
import random
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService

from config.config import USERNAME, PASSWORD, TIMEOUT, HEADLESS, DATA_DIR, SERIES_INDEX_FILE, SELECTORS_CONFIG

# Worker pool size — override via BS_MAX_WORKERS env var
MAX_WORKERS = int(os.getenv("BS_MAX_WORKERS", "16"))
USE_PARALLEL = True


# Pre-compiled regex for season label detection
_SEASON_LABEL_RE = re.compile(r'^(staffel|season|s)?\s*\d+$', re.IGNORECASE)
_SEASON_NUMBER_RE = re.compile(r'(staffel|season|s)\s*(\d+)', re.IGNORECASE)
_DOMAIN_STRIP_RE = re.compile(r'^https?://[^/]+')

# Navigation/utility pages to filter out from series listings
_UTILITY_PAGES = {'alle serien', 'andere serien', 'beliebte serien', 'neue serien', 'empfehlung', 'meistgesehen'}
_SERIE_PATH_RE = re.compile(r'(/serie/[^/]+)')


def is_regular_season(season_label):
    """True for numbered seasons (Staffel 1, Season 2, S3, etc.), False for specials."""
    return bool(_SEASON_LABEL_RE.search(season_label.strip()))


def _is_pid_alive(pid):
    """Check if a process with the given PID is still running."""
    try:
        if sys.platform == 'win32':
            result = subprocess.run(
                ['tasklist', '/FI', f'PID eq {pid}', '/NH'],
                capture_output=True, check=False, text=True
            )
            return str(pid) in result.stdout
        else:
            os.kill(pid, 0)
            return True
    except Exception:
        return False


def cleanup_stale_worker_pids():
    """Remove .worker_pids.json if all tracked processes are dead (e.g. after a hard kill).
    Called once on module startup to handle orphaned workers from previous runs."""
    worker_pids_file = os.path.join(DATA_DIR, '.worker_pids.json')
    if not os.path.exists(worker_pids_file):
        return
    try:
        with open(worker_pids_file, 'r') as f:
            pids = json.load(f)
        if not pids:
            os.remove(worker_pids_file)
            return
        any_alive = False
        for worker_id, pid in pids.items():
            if _is_pid_alive(pid):
                any_alive = True
                try:
                    if sys.platform == 'win32':
                        subprocess.run(
                            ['taskkill', '/F', '/PID', str(pid), '/T'],
                            capture_output=True, check=False
                        )
                    else:
                        subprocess.run(
                            ['kill', '-9', str(pid)],
                            capture_output=True, check=False
                        )
                except Exception:
                    pass
        os.remove(worker_pids_file)
    except Exception:
        try:
            os.remove(worker_pids_file)
        except Exception:
            pass


def cleanup_geckodriver_processes():
    """Kill geckodriver processes we spawned (tracked by PID file)."""
    worker_pids_file = os.path.join(DATA_DIR, '.worker_pids.json')
    if os.path.exists(worker_pids_file):
        try:
            with open(worker_pids_file, 'r') as f:
                pids = json.load(f)
            for worker_id, pid in pids.items():
                try:
                    if sys.platform == 'win32':
                        subprocess.run(
                            ['taskkill', '/F', '/PID', str(pid), '/T'],
                            capture_output=True, check=False
                        )
                    else:
                        subprocess.run(
                            ['kill', '-9', str(pid)],
                            capture_output=True, check=False
                        )
                except Exception:
                    pass
        except Exception:
            pass
        # Always remove the PID file after cleanup attempt
        try:
            os.remove(worker_pids_file)
        except Exception:
            pass


def _signal_handler(signum, frame):
    """Convert termination signals into clean exit so atexit handlers run"""
    sys.exit(0)


# Auto-clean stale PID file on startup (handles hard-killed terminals)
cleanup_stale_worker_pids()

# Register cleanup handlers
atexit.register(cleanup_geckodriver_processes)
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# On Windows, also handle SIGBREAK which is sent when the console window is closed.
# This ensures atexit handlers run and worker processes are cleaned up.
if sys.platform == 'win32':
    signal.signal(signal.SIGBREAK, _signal_handler)


class BsToScraper:
    """Config-based web scraper for BS.TO series"""
    
    def __init__(self):
        self.driver = None
        self.series_data = []
        self.config = SELECTORS_CONFIG
        self.auth_cookies = []
        self.checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')
        self.failed_file = os.path.join(DATA_DIR, '.failed_series.json')
        self.pause_file = os.path.join(DATA_DIR, '.pause_scraping')
        self.worker_pids_file = os.path.join(DATA_DIR, '.worker_pids.json')
        self.completed_links = set()
        self.failed_links = []
        self.worker_pids = {}  # {worker_id: geckodriver_pid}
        self._worker_lock = threading.Lock()
        self._checkpoint_mode = None
        
        if not self.config:
            raise Exception("selectors_config.json not loaded. Check config.py")
    
    # ==================== FILE I/O HELPERS ====================
    
    @staticmethod
    def _atomic_write_json(filepath, data):
        """Write JSON atomically via temp file + os.replace."""
        dirpath = os.path.dirname(filepath)
        os.makedirs(dirpath, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=dirpath, suffix='.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
            os.replace(tmp_path, filepath)
        except Exception:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise
    
    # ==================== CONFIG HELPERS ====================
    
    def get_selector(self, path):
        """Get selector from config using dot notation (e.g. 'login.username_field')."""
        keys = path.split('.')
        value = self.config.get('selectors', {})
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value
    
    def get_timing(self, key):
        return self.config.get('timing', {}).get(key, 0.3)
    
    def get_site_url(self):
        return self.config.get('site_url', 'https://bs.to')
    
    def is_logged_in(self, driver):
        """Check if driver session is authenticated by looking for known markers."""
        try:
            page_html = driver.page_source.lower()
            login_config = self.get_selector('login')
            markers = login_config.get('verification_markers', ['logout', 'hallo']) if login_config else ['logout']
            markers_to_check = [m.lower() for m in markers] + [USERNAME.lower()]
            return any(token in page_html for token in markers_to_check)
        except Exception:
            return False
    
    def get_login_page(self):
        return self.config.get('login_page', 'https://bs.to/login')
    
    def normalize_to_series_url(self, url):
        """Normalize any bs.to series URL to its canonical form (e.g. https://bs.to/serie/Name)."""
        url = _DOMAIN_STRIP_RE.sub("", url)
        m = _SERIE_PATH_RE.match(url)
        if m:
            main_path = m.group(1)
        else:
            return url
        site_url = self.get_site_url().rstrip("/")
        return f"{site_url}{main_path}"
    
    def parse_season_item(self, season_item):
        """Unpack a season tuple into (label, url, watched_status, season_type)."""
        # Handle new format (label, url, watched_status, season_type)
        if len(season_item) == 4:
            season_label, season_url, watched_status, season_type = season_item
        elif len(season_item) == 3:
            season_label, season_url, watched_status = season_item
            season_type = 'regular' if is_regular_season(season_label) else season_label
        else:
            season_label, season_url = season_item
            watched_status = "none"  # Unknown, needs loading
            season_type = 'regular' if is_regular_season(season_label) else season_label
        
        return season_label, season_url, watched_status, season_type
    
    # ==================== ELEMENT FINDING ====================
    
    def convert_selector_to_by(self, selector_type):
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
        """Try each selector from config until one matches."""
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
            except Exception:
                continue
        
        return None
    
    def wait_for_element(self, driver, selector_by, selector_value, timeout=None, silent=False):
        """Wait for element to be present. Returns True on success, False on timeout."""
        if timeout is None:
            # Increase default timeout to 20 seconds for more reliability
            timeout = self.get_timing('timeout') or 20
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((selector_by, selector_value))
            )
            return True
        except Exception as e:
            if not silent:
                print(f"✗ Timeout or error waiting for element: {selector_value} ({e})")
            return False
    
    def wait_for_css_element(self, driver, css_selector, timeout=None, silent=False):
        return self.wait_for_element(driver, By.CSS_SELECTOR, css_selector, timeout, silent)
    
    # ==================== CHECKPOINT SYSTEM ====================
    
    def save_checkpoint(self, include_data=False):
        """Save completed-links checkpoint for resume (atomic write).
        
        Args:
            include_data: If True, also save series_data for full state preservation.
                          Used on exit/crash. Periodic saves use False for speed.
        """
        try:
            checkpoint_data = {
                'completed_links': list(self.completed_links),
                'mode': self._checkpoint_mode,
                'timestamp': time.time(),
            }
            if include_data and self.series_data:
                checkpoint_data['series_data'] = self.series_data
            self._atomic_write_json(self.checkpoint_file, checkpoint_data)
        except Exception:
            pass
    
    def load_checkpoint(self):
        """Load checkpoint from a previous run. Returns True if loaded.
        
        Restores completed_links, mode, and series_data (if saved).
        """
        if not os.path.exists(self.checkpoint_file):
            return False
        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict) and 'completed_links' in data:
                self.completed_links = set(data.get('completed_links', []))
                self._checkpoint_mode = data.get('mode')
                saved_data = data.get('series_data', [])
                if saved_data:
                    self.series_data = saved_data
                return True
            elif isinstance(data, list):
                # Backward compatibility: treat as list of completed links
                self.completed_links = set(data)
                return True
            else:
                print(f"✗ Checkpoint file is invalid or corrupted.")
                return False
        except Exception as e:
            print(f"✗ Failed to load checkpoint: {e}")
            return False
    
    def clear_checkpoint(self):
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
        except Exception:
            pass

    @staticmethod
    def get_checkpoint_mode(data_dir):
        """Read checkpoint mode without fully loading the scraper."""
        path = os.path.join(data_dir, '.scrape_checkpoint.json')
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data.get('mode') if isinstance(data, dict) else None
        except Exception:
            return None
    
    def save_failed_series(self):
        """Persist failed series links for later retry (atomic write).
        
        Merges with any existing failed series from previous runs so no
        failures are lost across multiple scraping sessions.
        """
        if not self.failed_links:
            return
        try:
            existing = self.load_failed_series()
            # Merge: index existing by URL, then overlay with new failures
            def _url_key(item):
                if isinstance(item, dict):
                    return item.get('url', item.get('link', ''))
                return str(item)
            merged = {_url_key(item): item for item in existing}
            for item in self.failed_links:
                merged[_url_key(item)] = item
            self._atomic_write_json(self.failed_file, list(merged.values()))
        except Exception:
            pass
    
    def load_failed_series(self):
        try:
            if os.path.exists(self.failed_file):
                with open(self.failed_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data or []
        except Exception:
            pass
        return []
    
    def clear_failed_series(self):
        try:
            if os.path.exists(self.failed_file):
                os.remove(self.failed_file)
        except Exception:
            pass
    
    def is_pause_requested(self):
        return os.path.exists(self.pause_file)
    
    def clear_pause_request(self):
        try:
            if os.path.exists(self.pause_file):
                os.remove(self.pause_file)
        except Exception:
            pass
    
    def save_worker_pid(self, worker_id, pid):
        """Track a worker's geckodriver PID (thread-safe, atomic)."""
        with self._worker_lock:
            self.worker_pids[str(worker_id)] = pid
            try:
                self._atomic_write_json(self.worker_pids_file, dict(self.worker_pids))
            except Exception:
                pass
    
    def clear_worker_pids(self):
        with self._worker_lock:
            self.worker_pids = {}
            try:
                if os.path.exists(self.worker_pids_file):
                    os.remove(self.worker_pids_file)
            except Exception:
                pass
    
    # ==================== DRIVER SETUP ====================
    
    def _build_firefox_options(self):
        """Build shared Firefox options for main and worker drivers."""
        firefox_options = Options()
        if HEADLESS:
            firefox_options.add_argument("--headless")
        firefox_options.add_argument("--disable-gpu")
        firefox_options.add_argument('--disable-blink-features=AutomationControlled')
        firefox_options.set_preference("permissions.default.image", 1)
        firefox_options.set_preference("media.autoplay.default", 1)
        firefox_options.set_preference("dom.ipc.processPrelaunch.enabled", False)
        firefox_options.set_preference("network.http.speculative-parallel-limit", 0)
        firefox_options.set_preference(
            "general.useragent.override",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )
        return firefox_options
    
    def _build_firefox_service(self):
        """Build Firefox service, preferring local geckodriver if available."""
        gecko_path = os.path.join(os.path.dirname(__file__), '..', 'geckodriver.exe')
        if os.path.exists(gecko_path):
            return FirefoxService(gecko_path)
        return FirefoxService()
    
    def setup_driver(self):
        firefox_options = self._build_firefox_options()
        firefox_service = self._build_firefox_service()
        self.driver = webdriver.Firefox(service=firefox_service, options=firefox_options)
        
        # Track main driver PID for worker management
        try:
            if hasattr(firefox_service, 'process') and firefox_service.process:
                self.save_worker_pid(0, firefox_service.process.pid)  # worker_id=0 for main driver
        except Exception:
            pass
    
    def close(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            print("✓ Browser closed")
    
    # ==================== AUTHENTICATION ====================
    
    def login(self, driver=None):
        """Login to bs.to using config-based selectors."""
        drv = driver or self.driver
        try:
            login_config = self.get_selector('login')
            if not login_config:
                raise Exception("Login config not found in selectors_config.json")
            
            login_page = self.get_login_page()
            if drv is self.driver:
                print(f"→ Navigating to login page: {login_page}")
            drv.get(login_page)
            
            # Wait for login form to appear
            self.wait_for_css_element(drv, "input[type='submit'], button[type='submit']", timeout=3)

            # Username field
            username_field = self.find_element_from_config(
                drv, 
                login_config.get('username_field', []),
                timeout=3
            )
            if not username_field:
                raise Exception("Username field not found")
            username_field.send_keys(USERNAME)

            # Password field
            password_field = self.find_element_from_config(
                drv,
                login_config.get('password_field', []),
                timeout=3
            )
            if not password_field:
                raise Exception("Password field not found")
            password_field.send_keys(PASSWORD)

            # Submit button
            submit_button = self.find_element_from_config(
                drv,
                login_config.get('submit_button', []),
                timeout=2
            )

            if drv is self.driver:
                print("→ Submitting login...")
            if submit_button:
                submit_button.click()
            else:
                password_field.send_keys("\n")

            # Wait for redirect
            try:
                WebDriverWait(drv, TIMEOUT).until(
                    lambda d: d.current_url != login_page
                )
            except Exception:
                pass

            self.wait_for_css_element(drv, "body", timeout=3)
            time.sleep(self.get_timing('login_delay'))
            
            # Verify login
            if self.is_logged_in(drv):
                if drv is self.driver:
                    print("✓ Login completed")
                    try:
                        self.auth_cookies = drv.get_cookies()
                    except Exception:
                        self.auth_cookies = []
                return

            raise Exception(f"Login verification failed. URL: {drv.current_url}")

        except Exception as e:
            if drv is self.driver:
                print(f"✗ Login failed: {str(e)}")
            raise
    
    # ==================== SERIES DISCOVERY ====================
    
    def get_all_series(self):
        """Fetch the full series list from the andere-serien page."""
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
            seen = set()
            unique_series = []
            for s in series_list:
                # Skip utility/navigation pages (case-insensitive, strip whitespace)
                title_normalized = s['title'].lower().strip()
                # Also check if title contains utility keywords
                is_utility = (title_normalized in _UTILITY_PAGES or 
                             any(keyword in title_normalized for keyword in _UTILITY_PAGES))
                if is_utility:
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
        """Extract season links with watched status from the series page.

        Returns list of (label, url, watched_status, season_type) tuples.
        """
        soup = BeautifulSoup(html, 'html.parser')
        season_links = []

        series_config = self.get_selector('series_page')
        if not series_config:
            return []

        # Get config for season selector
        season_selector = series_config.get('season_selector', {})
        season_type = season_selector.get('type', 'css')
        season_value = season_selector.get('value')
        full_watched_class = season_selector.get('full_watched_class', 'watched')

        if not season_value:
            return []

        # Find all season links
        if season_type == 'css':
            season_elems = soup.select(season_value)
        else:
            season_elems = soup.find_all(season_value)

        site_url = self.get_site_url()
        for elem in season_elems:
            label = elem.get_text(strip=True)
            href = elem.get('href', '')
            
            # If href contains the series slug (serie/SeriesName/...), extract just the season/language part
            # href format: "serie/Breaking-Bad/0/de" → we want "0/de"
            if href.startswith('serie/'):
                parts = href.split('/')
                if len(parts) >= 3:
                    # Skip "serie" and series name, keep season number and language
                    href = '/'.join(parts[2:])
            
            classes = elem.get('class', [])
            if isinstance(classes, str):
                classes = classes.split()

            if full_watched_class and full_watched_class in classes:
                watched_status = 'full'  # All episodes watched (green) - skip loading
            else:
                watched_status = 'none'

            # Tag season type: 'regular' or special label
            if is_regular_season(label):
                season_type_val = 'regular'
            else:
                season_type_val = label

            # Build correct season URL using urljoin (handles all cases)
            if href.startswith('http'):
                season_url = href
            elif href:
                # Ensure base_url ends with / so urljoin treats it as a directory
                base_with_slash = base_url if base_url.endswith('/') else base_url + '/'
                season_url = urljoin(base_with_slash, href)
            else:
                season_url = base_url

            season_links.append((label, season_url, watched_status, season_type_val))

        # Deduplicate seasons while preserving order
        seen = set()
        unique = []
        for entry in season_links:
            # entry: (label, href, watched_status, season_type)
            key = (entry[0], entry[1])
            if key not in seen:
                seen.add(key)
                unique.append(entry)
        return unique
    
    # ==================== EPISODE SCRAPING ====================
    
    def scrape_episodes_from_html(self, html):
        """Parse episodes table and return list of {number, title, watched}."""
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
    
    def check_series_not_found_error(self, html):
        """Return the German error text if the page says 'Serie nicht gefunden'."""
        soup = BeautifulSoup(html, 'html.parser')
        error_div = soup.find('div', class_='messageBox error')
        if error_div:
            error_text = error_div.get_text(strip=True)
            if 'nicht gefunden' in error_text.lower():
                return error_text
        return None
    
    def process_series_page(self, url, series_hint=None):
        """Scrape a series using the main driver."""
        return self._process_series(self.driver, url, series_hint=series_hint)
    
    def extract_series_title(self, html):
        """Extract series title from HTML using config selectors."""
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
                main_text = element.get_text(strip=True)
                if element.small:
                    small_text = element.small.get_text(strip=True)
                    main_text = main_text.replace(small_text, "").strip()
                
                return main_text if main_text else None
        except Exception:
            pass
        
        return None
    
    def _process_series(self, driver, url, series_hint=None):
        """Core scraping: navigate to a series page, extract title, seasons, and episodes.

        Used by both sequential (self.driver) and parallel (worker) modes.
        """
        try:
            driver.get(url)
            self.wait_for_css_element(driver, "body", timeout=10)
            self.wait_for_css_element(driver, "h2", timeout=10, silent=True)

            page_content = driver.page_source

            # Detect browser error pages
            try:
                current_url = driver.current_url or ''
            except Exception:
                current_url = ''
            if current_url.startswith('about:neterror') or 'neterror' in current_url or 'dnsNotFound' in current_url:
                raise Exception(f"Reached error page: {current_url}")
            if page_content and ('Die Verbindung mit dem Server' in page_content or 'dnsNotFound' in page_content):
                raise Exception(f"Reached error page content for: {url}")

            # Check for "Serie nicht gefunden" error page
            error_found = self.check_series_not_found_error(page_content)
            if error_found:
                print(f"✗ Series not found: {url} - {error_found}")
                return {
                    'title': f'[ERROR: {error_found}]',
                    'url': url,
                    'total_episodes': 0,
                    'watched_episodes': 0,
                    'seasons': [],
                    'error': error_found,
                    'empty': True
                }

            # Extract title
            title_info = self.extract_series_title(page_content)
            title_value = title_info if title_info else None

            # Skip utility pages
            if title_value:
                title_normalized = title_value.lower().strip()
                utility_pages = _UTILITY_PAGES
                if title_normalized in utility_pages or any(keyword in title_normalized for keyword in utility_pages):
                    print(f"⚠ Skipping utility page: '{title_value}' (URL: {url})")
                    return {
                        'title': title_value,
                        'url': url,
                        'total_episodes': 0,
                        'watched_episodes': 0,
                        'seasons': []
                    }

            season_links = self.get_season_links(page_content, url)
            if not season_links:
                season_links = [("1", url, "none")]

            seasons_data = []
            total_watched = 0
            total_eps = 0

            for idx, season_item in enumerate(season_links):
                season_label, season_url, watched_status, season_type = self.parse_season_item(season_item)

                try:
                    max_retries = int(self.get_timing('max_retries_season') or 3)
                    episodes = []
                    season_failed = True
                    
                    for attempt in range(max_retries):
                        try:
                            driver.get(season_url)
                            time.sleep(self.get_timing('season_load_delay'))
                            self.wait_for_css_element(driver, "body", timeout=10)
                            silent = attempt < max_retries - 1
                            if self.wait_for_css_element(driver, "table.episodes", timeout=20 + attempt * 5, silent=silent):
                                season_html = driver.page_source
                                episodes = self.scrape_episodes_from_html(season_html)
                                season_failed = False
                                break
                            else:
                                if attempt < max_retries - 1:
                                    print(f"⚠ Retrying season {season_label} (attempt {attempt + 2}/{max_retries})")
                                    time.sleep(2)
                        except Exception as inner_e:
                            if attempt < max_retries - 1:
                                print(f"⚠ Error loading season {season_label}, retrying (attempt {attempt + 2}/{max_retries}): {inner_e}")
                                time.sleep(2)
                            else:
                                print(f"✗ Failed to load season {season_label} after {max_retries} attempts: {inner_e}")
                    
                    if not season_failed:
                        watched_count = sum(1 for ep in episodes if ep['watched'])
                        total_count = len(episodes)

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

            # Robust link assignment
            link = None
            if series_hint and series_hint.get('link'):
                link = series_hint.get('link')
            else:
                parsed = urlparse(url)
                m = _SERIE_PATH_RE.match(parsed.path)
                if m:
                    link = m.group(1)
                else:
                    link = parsed.path or url
            if not link.startswith('/'):
                link = '/' + link

            return {
                "title": title_value or (series_hint.get('title') if series_hint else url),
                "link": link,
                "url": url,
                "seasons": seasons_data,
                "watched_episodes": total_watched,
                "total_episodes": total_eps
            }
        except Exception as e:
            raise Exception(f"Series processing failed for {url}: {str(e)}")
    
    # ==================== SCRAPING MODES ====================
    
    def scrape_series_list(self):
        """Scrape all series in sequential or parallel mode."""
        try:
            # Initial delay
            time.sleep(self.get_timing('initial_delay'))
            
            all_series = self.get_all_series()

            if self._use_parallel:
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
    
    def _filter_completed(self, all_series):
        """Remove checkpoint-completed series. Returns filtered list or None if all done."""
        if not self.completed_links:
            return all_series
        before = len(all_series)
        filtered = [s for s in all_series if s.get('link') not in self.completed_links]
        if before != len(filtered):
            print(f"  Skipping {before - len(filtered)} already-completed series")
        if not filtered:
            print("✓ All series already scraped (from checkpoint)")
            return None
        return filtered

    def _scrape_series_sequential(self, all_series):
        """Sequential scraping with progress bar and ETA."""
        all_series = self._filter_completed(all_series)
        if all_series is None:
            return
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
                    
                    result = self.process_series_page(series['url'], series_hint=series)
                    if result:
                        season_labels = [s.get('season', '?') for s in result.get('seasons', [])]
                        season_info = f" [{','.join(season_labels)}]" if season_labels else ""
                        # Mark empty series
                        if result['total_episodes'] == 0:
                            result['empty'] = True
                            print(f"[{idx}/{len(all_series)}] [{bar}] {progress_pct}% | ETA: {eta_mins}m | ⚠ {result['title']}{season_info}: No episodes")
                        else:
                            result['empty'] = False
                            print(f"[{idx}/{len(all_series)}] [{bar}] {progress_pct}% | ETA: {eta_mins}m | ✓ {result['title']}{season_info}: {result['watched_episodes']}/{result['total_episodes']} watched")
                        self.series_data.append(result)
                        # Save checkpoint every 10 series
                        self.completed_links.add(series.get('link'))
                        if idx % 10 == 0:
                            self.save_checkpoint()
                    else:
                        print(f"[{idx}/{len(all_series)}] [{bar}] {progress_pct}% | ETA: {eta_mins}m | ⚠ {series['title']}: Skipped (no data)")
                except Exception as e:
                    print(f"  ⚠ Error processing {series['title']}: {str(e)}")
                    self.failed_links.append(series)
                    continue
        except KeyboardInterrupt:
            print("\n\n⚠ Scraping interrupted by user (Ctrl+C)")
            print(f"✓ Progress saved: {len(self.series_data)}/{len(all_series)} series scraped")
            print("→ Use 'Resume from checkpoint' option to continue later\n")
            return
        
        # Save failed series for later retry (sequential mode)
        if self.failed_links:
            self.save_failed_series()
            print(f"\n⚠ {len(self.failed_links)} series failed. Saved to .failed_series.json for retry.")

        # Final summary
        total_time = time.time() - start_time
        total_mins = int(total_time / 60)
        total_secs = int(total_time % 60)
        print(f"\n✓ Completed in {total_mins}m {total_secs}s", flush=True)
    
    def _scrape_series_parallel(self, all_series, worker_cap=None):
        """Parallel scraping with shared work queue and per-worker Firefox instances."""
        # Clear leftover pause file
        self.clear_pause_request()
        
        all_series = self._filter_completed(all_series)
        if all_series is None:
            return

        start_time = time.time()
        completed = 0
        failed = 0
        lock = threading.Lock()
        stop_event = threading.Event()

        # Filter utility pages (case-insensitive)
        filtered_series = [s for s in all_series if not any(keyword in s.get('title', '').lower().strip() for keyword in _UTILITY_PAGES)]
        total_series = len(filtered_series)

        max_workers_allowed = worker_cap if worker_cap is not None else MAX_WORKERS
        worker_count = min(max_workers_allowed, total_series) or 1
        
        # Shared work queue
        work_queue = queue.Queue()
        for item in filtered_series:
            work_queue.put(item)
        
        print(f"→ {total_series} series queued for {worker_count} workers (shared work queue)")

        def progress_line(done, total, title, watched=None, episode_total=None, empty=False, error=None, worker_id=None, season_labels=None):
            elapsed = time.time() - start_time
            avg = elapsed / max(1, done + failed)
            remaining = total - (done + failed)
            eta_mins = int((avg * remaining) / 60)
            pct = int(((done + failed) / total) * 100)
            bar_len = 30
            filled = int(bar_len * (done + failed) / total)
            bar = '█' * filled + '░' * (bar_len - filled)
            worker_info = f" | W{worker_id}/{worker_count}" if worker_id else f" | Workers: {worker_count}"
            season_info = f" [{','.join(season_labels)}]" if season_labels else ""
            if error:
                print(f"[{done+failed}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m{worker_info} | ✗ {title}: {error}")
            elif empty:
                print(f"[{done+failed}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m{worker_info} | ⚠ {title}{season_info}: No episodes")
            else:
                print(f"[{done+failed}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m{worker_info} | ✓ {title}{season_info}: {watched}/{episode_total} watched")

        def worker_loop(worker_id):
            nonlocal completed, failed
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
            
            # Share auth cookies from main driver
            authenticated = False
            max_auth_retries = 3
            for attempt in range(max_auth_retries):
                try:
                    if self._apply_cookies_to_driver(driver) and self.is_logged_in(driver):
                        authenticated = True
                        break
                    else:
                        # Cookie sharing failed, fall back to full login
                        self.login(driver)
                        driver.get(self.get_site_url())
                        time.sleep(1.0)
                        if self.is_logged_in(driver):
                            authenticated = True
                            break
                        else:
                            print(f"  ⚠ Worker #{worker_id}: Login verification failed (try {attempt + 1}/{max_auth_retries})")
                            time.sleep(1)
                except Exception as e:
                    print(f"  ⚠ Worker #{worker_id}: Auth failed - {str(e)[:80]}")
                    time.sleep(1)
                    continue
            
            if not authenticated:
                print(f"  ✗ Worker #{worker_id}: Failed to authenticate after {max_auth_retries} attempts. Items remain in queue for other workers.")
                # Close the driver before bailing out
                try:
                    driver.quit()
                except Exception:
                    pass
                return  # Exit worker — remaining items stay in queue for other workers

            # Pull work from queue until empty or stopped
            while not stop_event.is_set():
                try:
                    series = work_queue.get_nowait()
                except queue.Empty:
                    break  # No more work
                
                if stop_event.is_set():
                    break
                    
                # Check for pause request
                if self.is_pause_requested():
                    print(f"\n⏸ Worker #{worker_id} pausing (pause file detected)")
                    break

                try:
                    # Scrape the series
                    result = self._process_series(driver, series['url'], series)
                    with lock:
                        if result:
                            empty = result.get('total_episodes', 0) == 0
                            result['empty'] = empty
                            self.series_data.append(result)
                            self.completed_links.add(series.get('link'))
                            completed += 1
                            if completed % 10 == 0:
                                self.save_checkpoint()
                            watched = result.get('watched_episodes', 0)
                            total_eps = result.get('total_episodes', 0)
                            progress_line(completed, total_series, result.get('title', 'Series'), watched=watched, episode_total=total_eps, empty=empty, worker_id=worker_id, season_labels=[s.get('season', '?') for s in result.get('seasons', [])])
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
                        # Check login status on the current page first
                        if not self.is_logged_in(driver):
                            # Only reload main page if not logged in
                            driver.get(self.get_site_url())
                            if not self.is_logged_in(driver):
                                try:
                                    # Try cookies first, fall back to full login
                                    if not (self._apply_cookies_to_driver(driver) and self.is_logged_in(driver)):
                                        self.login(driver)
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
                            self.login(driver)
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
                print(f"  🔺 Worker #{worker_id} starting")
                futures.append(executor.submit(worker_loop, worker_id))

            # Wait for all workers to complete
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

        # Drain any remaining items from queue (e.g. all workers died)
        orphaned = 0
        while True:
            try:
                item = work_queue.get_nowait()
                self.failed_links.append(item)
                failed += 1
                orphaned += 1
            except queue.Empty:
                break
        if orphaned:
            print(f"  ⚠ {orphaned} series were not picked up by any worker — saved for retry")

        # Save final checkpoint to capture any progress since last periodic save
        self.save_checkpoint()

        # Check if pause was requested
        if self.is_pause_requested():
            print(f"\n⏸ Scraping paused by user")
            print(f"✓ Progress saved: {len(self.series_data)}/{total_series} series scraped")
            print(f"→ Resume later with checkpoint option or use 'Retry failed' for errors\n")
            self.clear_pause_request()
        
        # Save failed series for later retry
        if self.failed_links:
            self.save_failed_series()
            print(f"\n⚠ {len(self.failed_links)} series failed. Use 'Retry failed series' (option 6) to rescrape with a fresh login.")

        total_time = time.time() - start_time
        total_mins = int(total_time / 60)
        total_secs = int(total_time % 60)
        if failed:
            print(f"\n✓ Completed in {total_mins}m {total_secs}s ({failed} failed)", flush=True)
        else:
            print(f"\n✓ Completed in {total_mins}m {total_secs}s", flush=True)

    def _create_worker_driver(self, worker_id=None):
        """Create a new worker WebDriver and track its PID."""
        firefox_options = self._build_firefox_options()
        service = self._build_firefox_service()
        
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
        """Copy auth cookies to a worker driver (thread-safe snapshot)."""
        cookies_snapshot = list(self.auth_cookies)
        if not cookies_snapshot:
            return False
        try:
            driver.get(self.get_site_url())
            time.sleep(self.get_timing('action_delay'))
            for cookie in cookies_snapshot:
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
    
    # ==================== DATA MANAGEMENT ====================
    
    def load_existing_links(self):
        """Load existing series links from the index (for new-only filtering)."""
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
        """Scrape only series not yet in the index."""
        time.sleep(self.get_timing('initial_delay'))
        
        all_series = self.get_all_series()
        existing_links = self.load_existing_links()

        new_series_list = [s for s in all_series if s.get('link') not in existing_links]

        print(f"→ New series to scrape: {len(new_series_list)} (out of {len(all_series)})")
        self.series_data = []
        if not new_series_list:
            print("✓ No new series detected — skipping scraper spin-up")
            return

        # Run sequential for small delta set
        self._scrape_series_sequential(new_series_list)
    
    def scrape_retry_failed(self):
        """Retry previously failed series in sequential mode."""
        failed_list = self.load_failed_series()
        if not failed_list:
            print("✓ No failed series found")
            return
    
        print(f"✓ Found {len(failed_list)} failed series from last run")
        print("→ Starting retry in sequential mode (for reliability)...")
        self.series_data = []
    
        self._scrape_series_sequential(failed_list)
    # ==================== MAIN RUN METHODS ====================
    
    def scrape_single_series(self, url):
        """Scrape exactly one series by URL."""
        time.sleep(self.get_timing('initial_delay'))
        
        main_url = self.normalize_to_series_url(url)
        print(f"→ Scraping single series (normalized): {main_url}")
        result = self.process_series_page(main_url)
        self.series_data = [result]
        return result
    
    def scrape_multiple_series(self, urls):
        """Scrape multiple series from a URL list (sequential)."""
        time.sleep(self.get_timing('initial_delay'))
        
        print(f"→ Scraping {len(urls)} series from URL list (sequential mode)...")
        # Normalize all URLs to main series page
        series_list = []
        for url in urls:
            main_url = self.normalize_to_series_url(url)
            m = _SERIE_PATH_RE.search(main_url)
            link_path = m.group(1) if m else main_url
            series_list.append({'title': main_url.split('/')[-1], 'link': link_path, 'url': main_url})
        self.series_data = []
        self._scrape_series_sequential(series_list)
        print(f"  Successfully scraped: {len(self.series_data)}/{len(urls)} series")
    
    def run(self, single_url=None, url_list=None, new_only=False, resume_only=False, retry_failed=False, parallel=None):
        """Main entry point: setup driver, login, run selected mode, then close."""
        # Store parallel preference (thread-safe, no global mutation)
        if parallel is not None:
            self._use_parallel = parallel
            mode_str = "parallel" if parallel else "sequential"
            print(f"→ Using {mode_str} mode")
        else:
            self._use_parallel = USE_PARALLEL
        
        try:
            self.setup_driver()
            self.login()
            
            if resume_only:
                if self.load_checkpoint():
                    print(f"→ Resuming from checkpoint ({len(self.completed_links)} series already done)")
                else:
                    print("⚠ No checkpoint found. Starting fresh...")

            if single_url:
                self._checkpoint_mode = 'single'
                self.scrape_single_series(single_url)
            elif url_list:
                self._checkpoint_mode = 'batch'
                self.scrape_multiple_series(url_list)
            elif retry_failed:
                self._checkpoint_mode = 'retry'
                print("→ Running in 'retry failed series' mode")
                self.scrape_retry_failed()
            elif new_only:
                self._checkpoint_mode = 'new_only'
                print("→ Running in 'new series only' mode")
                self.scrape_new_series_only()
            else:
                self._checkpoint_mode = 'all_series'
                self.scrape_series_list()
            
            self.clear_checkpoint()
            # Only clear failed series file if no failures remain
            if not self.failed_links:
                self.clear_failed_series()
            else:
                self.save_failed_series()
        except BaseException:
            # Catches Exception, SystemExit (from SIGINT), KeyboardInterrupt
            # Save full checkpoint with series_data so no progress is lost on resume
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
            raise
        finally:
            self.clear_worker_pids()
            self.close()
