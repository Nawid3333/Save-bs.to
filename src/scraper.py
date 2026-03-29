"""
BS.TO Series Scraper

Config-driven scraper using Selenium + BeautifulSoup.
Supports sequential and parallel (ThreadPoolExecutor) modes,
checkpoints, retry, and atomic JSON writes.
"""

import atexit
import json
import logging
import os
import queue
import random
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.common.exceptions import TimeoutException, WebDriverException

from config.config import USERNAME, PASSWORD, HEADLESS, DATA_DIR, SERIES_INDEX_FILE, SELECTORS_CONFIG

logger = logging.getLogger(__name__)

# Worker pool size — override via BS_MAX_WORKERS env var
MAX_WORKERS = int(os.getenv("BS_MAX_WORKERS", "24"))
USE_PARALLEL = True

# Checkpoint frequency (save progress every N series)
CHECKPOINT_EVERY = 10

# Max retries for worker authentication
MAX_AUTH_RETRIES = 3


# Pre-compiled regex for season label detection
_SEASON_LABEL_RE = re.compile(r'^(staffel|season|s)?\s*\d+$', re.IGNORECASE)
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
                capture_output=True, check=False, text=True,
                encoding='utf-8', errors='replace'
            )
            return str(pid) in result.stdout
        else:
            os.kill(pid, 0)
            return True
    except (OSError, ValueError):
        return False


def _kill_pids_in_file(pids_dict):
    """Kill all geckodriver PIDs listed in a pids dict (skips _owner_pid)."""
    for key, pid in pids_dict.items():
        if key == '_owner_pid':
            continue
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
        except (OSError, subprocess.SubprocessError):
            pass


def cleanup_stale_worker_pids():
    """Scan data/ for all per-process .worker_pids_<pid>.json files.
    For each: if the owning Python process is dead, kill geckodriver orphans and remove the file.
    Called once on module startup to handle orphaned workers from previous runs."""
    try:
        files = [
            f for f in os.listdir(DATA_DIR)
            if f.startswith('.worker_pids_') and f.endswith('.json')
        ]
    except OSError:
        return
    for fname in files:
        fpath = os.path.join(DATA_DIR, fname)
        try:
            with open(fpath, 'r') as f:
                pids = json.load(f)
            if not isinstance(pids, dict):
                os.remove(fpath)
                continue
            owner_pid = pids.get('_owner_pid')
            if owner_pid and _is_pid_alive(owner_pid):
                # Owning Python process is still alive — another live instance, don't interfere
                continue
            # Owner is dead — kill geckodriver orphans and clean up
            _kill_pids_in_file(pids)
            os.remove(fpath)
        except (OSError, json.JSONDecodeError, ValueError):
            try:
                os.remove(fpath)
            except OSError:
                pass


def cleanup_geckodriver_processes():
    """Kill geckodriver processes we spawned (tracked by this process's own PID file)."""
    if os.path.exists(_MY_PID_FILE):
        try:
            with open(_MY_PID_FILE, 'r') as f:
                pids = json.load(f)
            if isinstance(pids, dict):
                _kill_pids_in_file(pids)
        except (OSError, json.JSONDecodeError, ValueError):
            pass
        try:
            os.remove(_MY_PID_FILE)
        except OSError:
            pass


def _signal_handler(signum, frame):
    """Convert termination signals into clean exit so atexit handlers run"""
    sys.exit(0)


# Per-process PID file — each running instance gets its own file so they don't stomp each other
_MY_PID_FILE = os.path.join(DATA_DIR, f'.worker_pids_{os.getpid()}.json')

# NOTE: cleanup_stale_worker_pids() is called once on first scraper
# instantiation via BsToScraper.__init__(), not at import time.

# Register cleanup handlers
atexit.register(cleanup_geckodriver_processes)
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# On Windows, also handle SIGBREAK which is sent when the console window is closed.
# This ensures atexit handlers run and worker processes are cleaned up.
if sys.platform == 'win32':
    signal.signal(signal.SIGBREAK, _signal_handler)


class ScrapingPaused(Exception):
    """Raised when scraping is paused via pause file. Triggers checkpoint save in run()."""
    pass


class BsToScraper:
    """Config-based web scraper for BS.TO series"""

    _stale_pids_cleaned = False
    
    def __init__(self):
        if not BsToScraper._stale_pids_cleaned:
            cleanup_stale_worker_pids()
            BsToScraper._stale_pids_cleaned = True

        self.driver = None
        self.series_data = []
        self.config = SELECTORS_CONFIG
        self.auth_cookies = []
        self.checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')
        self.failed_file = os.path.join(DATA_DIR, '.failed_series.json')
        self.pause_file = os.path.join(DATA_DIR, '.pause_scraping')
        self.worker_pids_file = _MY_PID_FILE
        self.completed_links = set()
        self.failed_links = []
        self.worker_pids = {}  # {worker_id: geckodriver_pid}
        self._worker_lock = threading.Lock()
        self._checkpoint_mode = None
        self._use_parallel = USE_PARALLEL
        self._season_max_retries = int(self.config.get('timing', {}).get('max_retries_season') or 0) or 3
        self._last_pause_check = 0.0
        self._pause_cached = False
        self.all_discovered_series = None
        self.timing_file = os.path.join(DATA_DIR, '.scrape_timing.json')
        self._historical_avg = None  # Loaded at scrape start from last run
        
        if not self.config:
            raise Exception("selectors_config.json not loaded. Check config.py")
    
    # ==================== SCRAPE TIMING HELPERS ====================

    def _load_scrape_timing(self):
        """Load avg time per series from last completed scrape."""
        try:
            with open(self.timing_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            avg = data.get('avg_per_series')
            if avg and avg > 0:
                return float(avg)
        except (OSError, json.JSONDecodeError, ValueError):
            pass
        return None

    def _save_scrape_timing(self, duration, series_count):
        """Save timing data from completed scrape for future ETA estimates."""
        if series_count <= 0:
            return
        data = {
            'last_scrape_duration': round(duration, 2),
            'series_count': series_count,
            'avg_per_series': round(duration / series_count, 4),
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S')
        }
        try:
            self._atomic_write_json(self.timing_file, data)
        except Exception as e:
            logger.warning(f"Could not save scrape timing: {e}")

    @staticmethod
    def _compute_eta_mins(done, total, elapsed, historical_avg=None):
        """Compute ETA in minutes, blending historical avg with current session.

        At the start of a scrape the current-session average is unreliable
        (based on only 1-2 samples).  When *historical_avg* is available
        (from the previous completed scrape), the estimate starts from
        the historical value and smoothly transitions to the live average
        over the first 15% of the total series count.
        """
        if done <= 0:
            if historical_avg:
                return int((historical_avg * total) / 60)
            return 0
        current_avg = elapsed / done
        if historical_avg is None:
            effective_avg = current_avg
        else:
            blend = min(1.0, done / max(1, total * 0.15))
            effective_avg = (1 - blend) * historical_avg + blend * current_avg
        remaining = total - done
        return int((effective_avg * remaining) / 60)

    # ==================== FILE I/O HELPERS ====================
    
    @staticmethod
    def _atomic_write_json(filepath, data):
        """Write JSON atomically via temp file + os.replace.
        
        Also checks disk space before writing to prevent corruption.
        """
        dirpath = os.path.dirname(filepath)
        os.makedirs(dirpath, exist_ok=True)
        
        # Check disk space (need at least 1 MB free for writing)
        try:
            stat = shutil.disk_usage(dirpath)
            if stat.free < 1024 * 1024:
                raise OSError(f"Insufficient disk space for {filepath} (< 1 MB free)")
        except OSError:
            raise
        except Exception as e:
            logger.warning(f"Could not check disk space: {e}")
        
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
    
    def get_timing(self, key, default=0.3):
        return self.config.get('timing', {}).get(key, default)
    
    def get_site_url(self):
        return self.config.get('site_url', 'https://bs.to')
    
    def _is_driver_alive(self, driver=None):
        """Check if a WebDriver session is still usable."""
        drv = driver or self.driver
        if drv is None:
            return False
        try:
            _ = drv.current_url
            return True
        except Exception:
            return False

    def _has_auth_cookies(self, driver):
        """Lightweight auth check: verify session cookies exist without page navigation.

        Much faster than is_logged_in() which loads the page. Use for periodic
        health checks; reserve is_logged_in() for error recovery.

        Returns:
            bool: True if session cookies are present
        """
        try:
            cookies = driver.get_cookies()
            cookie_names = {c['name'] for c in cookies}
            # bs.to is PHP-based — check for PHP session cookies
            session_indicators = {'PHPSESSID', 'session'}
            if cookie_names & session_indicators:
                return True
            # Fallback: if we have 2+ cookies on the bs.to domain, session is likely alive
            site_domain = urlparse(self.get_site_url()).hostname
            domain_cookies = [c for c in cookies if site_domain in (c.get('domain', '') or '')]
            if len(domain_cookies) >= 2:
                return True
            logger.debug(f"_has_auth_cookies: no session indicators found. Cookies: {cookie_names}")
            return False
        except Exception:
            return False

    def is_logged_in(self, driver):
        """Check if authenticated using configurable selector from login config."""
        try:
            login_config = self.get_selector('login') or {}
            indicator = login_config.get('logged_in_indicator', "section.navigation a[href='logout']")
            return len(driver.find_elements(By.CSS_SELECTOR, indicator)) > 0
        except Exception:
            return False
    
    def get_login_page(self):
        return self.config.get('login_page', 'https://bs.to/login')
    
    def normalize_to_series_url(self, url):
        """Normalize any bs.to series URL to its canonical form (e.g. https://bs.to/serie/Name).
        Strips fragments and query strings.
        """
        if not url:
            return url
        
        # Strip fragment (#) and query string (?) from URL
        url = url.split('?')[0].split('#')[0]
        
        # Remove domain, keep path only
        url = _DOMAIN_STRIP_RE.sub("", url)
        
        # Extract /serie/Name part
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

    def get_timing_float(self, key, default, min_val=0.0, max_val=None):
        """Read a timing value from config as float (safe with None/invalid values).
        
        Args:
            key: Config key to retrieve
            default: Default value if missing or invalid
            min_val: Minimum allowed value (default: 0.0)
            max_val: Maximum allowed value (default: None, no limit)
        """
        try:
            value = self.get_timing(key, default)
            if value is None or (isinstance(value, str) and value.lower() in ('null', 'none')):
                return float(default) if default is not None else 0.0
            result = float(value)
            if min_val is not None:
                result = max(result, min_val)
            if max_val is not None:
                result = min(result, max_val)
            return result
        except (ValueError, TypeError):
            logger.warning(f"Invalid timing value for {key}: {value}, using default {default}")
            return float(default) if default is not None else 0.0

    def get_timing_int(self, key, default, min_val=0, max_val=None):
        """Read a timing value from config as int (safe with None/invalid values).
        
        Args:
            key: Config key to retrieve
            default: Default value if missing or invalid
            min_val: Minimum allowed value (default: 0)
            max_val: Maximum allowed value (default: None, no limit)
        """
        try:
            value = self.get_timing(key, default)
            if value is None or (isinstance(value, str) and value.lower() in ('null', 'none')):
                return int(default) if default is not None else 0
            result = int(float(value))
            if min_val is not None:
                result = max(result, min_val)
            if max_val is not None:
                result = min(result, max_val)
            return result
        except (ValueError, TypeError):
            logger.warning(f"Invalid timing value for {key}: {value}, using default {default}")
            return int(default) if default is not None else 0
    
    def find_element_from_config(self, driver, config_selectors, timeout=None):
        """Try each selector from config until one matches."""
        if not isinstance(config_selectors, list):
            config_selectors = [config_selectors]
        if timeout is None:
            timeout = self.get_timing_float('element_find_timeout', 2.0)
        
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
            timeout = self.get_timing_float('timeout', 20.0)
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
    
    def _wait_for_page_ready(self, driver=None, timeout=None):
        """Wait for page to be fully loaded (DOM readyState == 'complete' + body present).
        Returns as soon as the page is ready — no fixed sleep."""
        drv = driver or self.driver
        if timeout is None:
            timeout = self.get_timing_float('page_ready_timeout', 10.0)
        body_timeout = min(self.get_timing_float('page_ready_body_timeout', 3.0), timeout)
        try:
            WebDriverWait(drv, timeout).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
        except Exception:
            pass
        try:
            WebDriverWait(drv, body_timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, 'body'))
            )
        except Exception:
            pass
    
    # ==================== CHECKPOINT SYSTEM ====================
    
    def save_checkpoint(self, include_data=False):
        """Save completed-links checkpoint for resume (atomic write, thread-safe).
        
        Args:
            include_data: If True, also save series_data for full state preservation.
                          Used on exit/crash. Periodic saves use False.
        """
        with self._worker_lock:
            try:
                checkpoint_data = {
                    'completed_links': list(self.completed_links),
                    'mode': self._checkpoint_mode,
                    'timestamp': time.time(),
                }
                if include_data and self.series_data:
                    checkpoint_data['series_data'] = self.series_data
                self._atomic_write_json(self.checkpoint_file, checkpoint_data)
            except Exception as e:
                logger.error(f"Failed to save checkpoint: {e}")
                print(f"  ⚠ Warning: checkpoint save failed: {e}")
    
    def load_checkpoint(self):
        """Load checkpoint from a previous run (thread-safe). Returns True if loaded.
        
        Restores completed_links, mode, and series_data (if saved).
        """
        with self._worker_lock:
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
        """Clear checkpoint after successful completion (thread-safe)."""
        with self._worker_lock:
            try:
                if os.path.exists(self.checkpoint_file):
                    os.remove(self.checkpoint_file)
            except OSError as e:
                logger.debug(f"Could not remove checkpoint file: {e}")

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
        except (json.JSONDecodeError, OSError) as e:
            logger.debug(f"Could not read checkpoint mode: {e}")
            return None
    
    def save_failed_series(self):
        """Persist failed series links for later retry (atomic write, thread-safe).
        
        Merges with any existing failed series from previous runs so no
        failures are lost across multiple scraping sessions.
        """
        if not self.failed_links:
            return
        with self._worker_lock:
            try:
                existing = self._load_failed_series_unlocked()
                # Merge: index existing by URL, then overlay with new failures
                def _url_key(item):
                    if isinstance(item, dict):
                        return item.get('url', item.get('link', ''))
                    return str(item)
                merged = {_url_key(item): item for item in existing if _url_key(item)}
                for item in self.failed_links:
                    key = _url_key(item)
                    if key:
                        merged[key] = item
                    else:
                        logger.warning(f"Skipping failed item with empty URL: {item}")
                self._atomic_write_json(self.failed_file, list(merged.values()))
            except Exception as e:
                logger.error(f"Failed to save failed series list: {e}")
                print(f"  ⚠ Warning: could not save failed series list: {e}")
    
    def _load_failed_series_unlocked(self):
        """Internal: load failed series without locking (for use within locked context)."""
        try:
            with open(self.failed_file, 'r', encoding='utf-8') as f:
                return json.load(f) or []
        except FileNotFoundError:
            return []
        except json.JSONDecodeError as e:
            logger.warning(f"Failed series file corrupted, ignoring: {e}")
            return []
        except (OSError, TypeError) as e:
            logger.warning(f"Could not load failed series: {e}")
            return []

    def load_failed_series(self):
        """Load previously failed series for retry (thread-safe)."""
        with self._worker_lock:
            return self._load_failed_series_unlocked()
    
    def clear_failed_series(self):
        """Clear failed series list after successful retry (thread-safe)."""
        with self._worker_lock:
            try:
                if os.path.exists(self.failed_file):
                    os.remove(self.failed_file)
            except OSError as e:
                logger.debug(f"Could not remove failed series file: {e}")
    
    def is_pause_requested(self):
        """Check if pause was requested (cached: re-checks file at most every 2 seconds)."""
        try:
            now = time.time()
            if now - self._last_pause_check < 2.0:
                return self._pause_cached
            self._last_pause_check = now
            self._pause_cached = os.path.exists(self.pause_file)
            return self._pause_cached
        except OSError:
            return False
    
    def clear_pause_request(self):
        """Remove pause request file and reset cache."""
        self._pause_cached = False
        self._last_pause_check = 0.0
        try:
            if os.path.exists(self.pause_file):
                os.remove(self.pause_file)
        except OSError as e:
            logger.debug(f"Could not remove pause file: {e}")
    
    def save_worker_pid(self, worker_id, pid):
        """Track a worker's geckodriver PID (thread-safe, atomic)."""
        with self._worker_lock:
            self.worker_pids[str(worker_id)] = pid
            try:
                payload = {'_owner_pid': os.getpid()}
                payload.update(self.worker_pids)
                self._atomic_write_json(self.worker_pids_file, payload)
            except Exception as e:
                logger.debug(f"Failed to save worker PID {worker_id}: {e}")
    
    def clear_worker_pids(self):
        with self._worker_lock:
            self.worker_pids = {}
            try:
                if os.path.exists(self.worker_pids_file):
                    os.remove(self.worker_pids_file)
            except OSError as e:
                logger.debug(f"Could not remove worker PIDs file: {e}")
    
    # ==================== DRIVER SETUP ====================
    
    def _build_firefox_options(self):
        """Build shared Firefox options for main and worker drivers."""
        firefox_options = Options()
        if HEADLESS:
            firefox_options.add_argument("--headless")
        firefox_options.add_argument("--disable-gpu")
        firefox_options.add_argument('--disable-blink-features=AutomationControlled')
        
        # Performance preferences — disable slow startup features
        firefox_options.set_preference("startup.homepage_welcome_url", "")
        firefox_options.set_preference("startup.homepage_welcome_url.additional", "")
        firefox_options.set_preference("browser.startup.homepage_override.mstone", "ignore")
        firefox_options.set_preference("browser.startup.homepage", "about:blank")
        firefox_options.set_preference("browser.cache.check_doc_frequency", 0)  # Skip cache validation
        firefox_options.set_preference("app.update.auto", False)  # No auto-update checks
        firefox_options.set_preference("browser.sessionstore.max_tabs_undo", 0)  # Fast session restore
        
        # Ad/media blocking preferences
        firefox_options.set_preference("permissions.default.image", 1)
        firefox_options.set_preference("media.autoplay.default", 1)
        firefox_options.set_preference("dom.ipc.processPrelaunch.enabled", False)
        firefox_options.set_preference("network.http.speculative-parallel-limit", 0)
        firefox_options.set_preference(
            "general.useragent.override",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )
        return firefox_options
    
    def _get_ublock_xpi(self):
        """Find uBlock Origin .xpi, using local copy or copying from Firefox profile."""
        ublock_id = 'uBlock0@raymondhill.net.xpi'
        addon_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'addons')
        local_xpi = os.path.join(addon_dir, 'ublock_origin.xpi')
        
        if os.path.isfile(local_xpi):
            return local_xpi
        
        # Search Firefox profiles for installed uBlock
        profiles_dir = os.path.join(os.environ.get('APPDATA', ''), 'Mozilla', 'Firefox', 'Profiles')
        if os.path.isdir(profiles_dir):
            for profile in os.listdir(profiles_dir):
                xpi_path = os.path.join(profiles_dir, profile, 'extensions', ublock_id)
                if os.path.isfile(xpi_path):
                    os.makedirs(addon_dir, exist_ok=True)
                    shutil.copy2(xpi_path, local_xpi)
                    print(f'✓ Copied uBlock Origin from Firefox profile: {profile}')
                    return local_xpi
        
        # Download from Mozilla Add-ons as last resort
        url = 'https://addons.mozilla.org/firefox/downloads/latest/ublock-origin/latest.xpi'
        print('→ Downloading uBlock Origin from addons.mozilla.org...')
        try:
            os.makedirs(addon_dir, exist_ok=True)
            urllib.request.urlretrieve(url, local_xpi)
            print('✓ uBlock Origin downloaded')
            return local_xpi
        except Exception as e:
            print(f'⚠ Failed to download uBlock Origin: {e}')
            try:
                os.remove(local_xpi)
            except OSError:
                pass
            return None
    
    def _install_ublock(self, driver):
        """Install uBlock Origin into a driver instance."""
        xpi = self._get_ublock_xpi()
        if xpi:
            try:
                driver.install_addon(xpi, temporary=True)
                time.sleep(self.get_timing_float('addon_init_delay', 0.1))
            except Exception as e:
                logger.debug(f'Failed to install uBlock Origin: {e}')
    
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
        self.driver.set_page_load_timeout(self.get_timing_float('page_load_timeout', 20.0))
        
        # Track main driver PID for worker management
        try:
            if hasattr(firefox_service, 'process') and firefox_service.process:
                self.save_worker_pid(0, firefox_service.process.pid)  # worker_id=0 for main driver
        except Exception:
            pass
        
        # Install uBlock Origin for ad-blocking (faster page loads)
        self._install_ublock(self.driver)
    
    def close(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            print("✓ Browser closed")
    
    # ==================== AUTHENTICATION ====================
    
    def login(self, driver=None, retry_count=0, max_retries=2):
        """Login to bs.to using JS injection.
        
        Targets the main #login-captcha form on /login page.
        Falls back to header #login form if main form not found.
        """
        drv = driver or self.driver
        try:
            login_page = self.get_login_page()
            if drv is self.driver:
                print(f"→ Navigating to login page: {login_page}")
            drv.get(login_page)
            self._wait_for_page_ready(drv, timeout=self.get_timing_float('login_page_ready_timeout', 5.0))
            
            # Grab a reference to an element on the current page before submit
            old_html = drv.find_element(By.TAG_NAME, 'html')
            
            # Fill and submit via JS — targets #login-captcha (main form) or #login (header)
            drv.execute_script("""
                var form = document.getElementById('login-captcha') || document.getElementById('login');
                if (!form) throw new Error('Login form not found');
                var user = form.querySelector("input[name='login[user]']");
                var pass = form.querySelector("input[name='login[pass]']");
                if (!user || !pass) throw new Error('Login fields not found');
                user.value = arguments[0];
                pass.value = arguments[1];
                form.submit();
            """, USERNAME, PASSWORD)

            # Wait for the page to reload (old element becomes stale)
            try:
                WebDriverWait(drv, self.get_timing_float('login_response_timeout', 10.0)).until(EC.staleness_of(old_html))
            except Exception:
                pass
            
            # Wait for login response (bs.to stays on /login URL but renders home page)
            self._wait_for_page_ready(drv, timeout=self.get_timing_float('login_response_timeout', 10.0))
            
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
            if retry_count < max_retries:
                logger.warning(f"Login attempt {retry_count + 1} failed: {e}, retrying...")
                time.sleep(self.get_timing_float('worker_auth_retry_delay', 1.0))
                return self.login(drv, retry_count=retry_count + 1, max_retries=max_retries)
            if drv is self.driver:
                print(f"✗ Login failed after {retry_count + 1} attempts: {str(e)}")
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
            self._wait_for_page_ready(self.driver)
            
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
            
            # Remove duplicates while preserving order (by slug for resilience)
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
                slug = self.get_series_slug_from_url(s['link'])
                if slug != 'unknown' and slug not in seen:
                    seen.add(slug)
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
            try:
                label = elem.get_text(strip=True)
                if not label:  # Skip elements with no text
                    continue
                
                href = elem.get('href', '')
                if not href:  # Skip elements with no href
                    continue
                
                # Strip query strings and fragments from href
                href = href.split('?')[0].split('#')[0]
                
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
                elif not classes:
                    classes = []

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
            except Exception:
                # Skip any malformed season elements
                continue

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
        """Parse episodes table and return list of {number, title, watched}.
        Handles missing fields, None values, and malformed rows robustly.
        
        Returns:
            tuple: (episodes_list, malformed_count) where malformed_count is
                   the number of rows that failed to parse.
        """
        soup = BeautifulSoup(html, 'html.parser')
        episodes = []
        malformed_count = 0
        
        episode_config = self.get_selector('episodes')
        if not episode_config or not isinstance(episode_config, dict):
            return episodes, malformed_count
        
        table_config = episode_config.get('table', {})
        if not isinstance(table_config, dict):
            return episodes, malformed_count
            
        table_type = table_config.get('type', 'css')
        table_value = table_config.get('value')
        
        # Validate selector value exists and is non-empty
        if not table_value:
            return episodes, malformed_count
        
        try:
            if table_type == 'css':
                table = soup.select_one(str(table_value))
            else:
                table = soup.find(str(table_value))
        except Exception:
            return episodes, malformed_count
        
        if not table:
            return episodes, malformed_count
        
        try:
            row_config = episode_config.get('table_rows', {})
            row_type = row_config.get('type', 'css')
            row_value = row_config.get('value')
            
            if row_value:
                rows = table.select(str(row_value)) if row_type == 'css' else table.find_all(str(row_value))
            else:
                rows = table.find_all('tr')
            
            watched_indicator = episode_config.get('watched_indicator', {})
            indicator_type = watched_indicator.get('type', 'row_class')
            indicator_value = watched_indicator.get('value', 'watched')
            
            ep_num_cell = episode_config.get('episode_number_cell', 0)
            ep_title_cell = episode_config.get('episode_title_cell', 1)
            ep_title_selector = episode_config.get('episode_title_selector', 'strong')
            
            # Validate cell indices are non-negative
            if ep_num_cell < 0 or ep_title_cell < 0:
                return episodes, malformed_count
            
            for row_idx, row in enumerate(rows, start=1):
                try:
                    cols = row.find_all('td')
                    if not cols or len(cols) <= max(ep_num_cell, ep_title_cell):
                        continue
                    
                    ep_num = cols[ep_num_cell].get_text(strip=True)
                    if not ep_num:
                        # Filme/movie rows may lack episode numbers — fall back
                        # to data-episode-season-id (ordinal number within season),
                        # then to 1-based row index.
                        ep_num = row.get('data-episode-season-id', '')
                    if not ep_num:
                        ep_num = str(row_idx)
                        logger.debug(f"Episode number fallback to row index {row_idx}")
                    
                    title_col = cols[ep_title_cell]
                    title_tag = title_col.find(ep_title_selector) if ep_title_selector else None
                    title = title_tag.get_text(strip=True) if title_tag else ''
                    
                    # Detect watched status (safe against missing/None values)
                    watched = False
                    if indicator_type == 'row_class':
                        row_classes = row.get('class', [])
                        if isinstance(row_classes, str):
                            row_classes = row_classes.split()
                        watched = bool(indicator_value and indicator_value in row_classes)
                    
                    # Normalize episode number to string for consistent storage
                    episodes.append({
                        'number': str(ep_num),
                        'title': str(title) if title else '',
                        'watched': bool(watched)  # Ensure bool type
                    })
                except Exception as e:
                    malformed_count += 1
                    logger.warning(f"Malformed episode row {row_idx}: {e}")
                    continue
        except Exception:
            pass  # Return whatever episodes were parsed before error
        
        return episodes, malformed_count
    
    # ==================== SERIES PROCESSING ====================

    _ERROR_TITLE_RE = re.compile(
        r'^(?:Error\s+)?(?P<code>\d{3})\b|\b(?:Error|Fehler)\s+(?P<code2>\d{3})\b',
        re.IGNORECASE,
    )

    _SERVER_ERROR_CODES = {
        '429': '429 Too Many Requests',
        '500': '500 Internal Server Error',
        '502': '502 Bad Gateway',
        '503': '503 Service Unavailable',
        '504': '504 Gateway Timeout',
    }

    def check_series_not_found_error(self, html):
        """Return the error text if the page indicates series not found (404)."""
        soup = BeautifulSoup(html, 'html.parser')
        error_div = soup.find('div', class_='messageBox error')
        if error_div:
            error_text = error_div.get_text(strip=True)
            if 'nicht gefunden' in error_text.lower():
                return error_text
        # If the page has series content (season links), it's a real series
        # page — not an error page. This prevents false positives for series
        # named "Error 404", "Fehler 404", etc.
        if soup.select_one('#seasons a'):
            return None
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            m = self._ERROR_TITLE_RE.search(title_text)
            if m:
                code = m.group('code') or m.group('code2')
                if code == '404':
                    return title_text
        h2_tag = soup.find('h2')
        if h2_tag and h2_tag.get_text(strip=True) == '404':
            p_tag = soup.find('p')
            return p_tag.get_text(strip=True) if p_tag else '404 Nicht gefunden'
        return None

    def check_server_error(self, html):
        """Check if page contains a server error (429, 500, 502, 503, 504)."""
        soup = BeautifulSoup(html, 'html.parser')
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            m = self._ERROR_TITLE_RE.search(title_text)
            if m:
                code = m.group('code') or m.group('code2')
                if code in self._SERVER_ERROR_CODES:
                    return self._SERVER_ERROR_CODES[code]
        body_text = soup.get_text(strip=True) if soup.body else ''
        for code, message in self._SERVER_ERROR_CODES.items():
            reason = message.split(' ', 1)[1]
            if code in body_text and reason in body_text:
                return message
        return None
    
    def process_series_page(self, url, series_hint=None):
        """Scrape a series using the main driver."""
        return self._process_series(self.driver, url, series_hint=series_hint)
    
    def extract_series_title(self, html):
        """Extract series title from HTML using config selectors with fallbacks.
        
        Tries config selectors first, then falls back to common title patterns.
        Returns title string or None if extraction fails completely.
        """
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        series_config = self.get_selector('series_page')
        if series_config and isinstance(series_config, dict):
            title_config = series_config.get('title', {})
            if isinstance(title_config, dict):
                title_type = title_config.get('type', 'tag')
                title_value = title_config.get('value', 'h2')
                
                try:
                    if title_value:
                        if title_type == 'tag':
                            element = soup.find(title_value)
                        else:
                            element = soup.select_one(str(title_value))
                        
                        if element:
                            main_text = element.get_text(strip=True)
                            # Remove subtitle if present (often in <small> tag)
                            small_elem = element.find('small')
                            if small_elem:
                                small_text = small_elem.get_text(strip=True)
                                main_text = main_text.replace(small_text, "").strip()
                            
                            if main_text:
                                return main_text
                except Exception as e:
                    logger.debug(f"Config-based title extraction failed: {e}")
        
        # Fallback: try common title patterns
        try:
            # Try h1, h2, h3 tags in order
            for tag in ['h1', 'h2', 'h3']:
                element = soup.find(tag)
                if element:
                    text = element.get_text(strip=True)
                    if text and len(text) > 2:  # Avoid very short titles
                        return text
            
            # Try meta tags as last resort
            og_title = soup.find('meta', property='og:title')
            if og_title and og_title.get('content'):
                return og_title.get('content').strip()
            
            title_tag = soup.find('title')
            if title_tag:
                text = title_tag.get_text(strip=True)
                # Remove common site prefixes (e.g., "bs.to | Series Name")
                if '|' in text:
                    text = text.split('|', 1)[1].strip()
                if text and len(text) > 2:
                    return text
        except Exception as e:
            logger.debug(f"Fallback title extraction failed: {e}")
        
        return None
    
    def _process_series(self, driver, url, series_hint=None):
        """Core scraping: navigate to a series page, extract title, seasons, and episodes.

        Used by both sequential (self.driver) and parallel (worker) modes.
        """
        try:
            driver.get(url)
            self._wait_for_page_ready(driver)
            self.wait_for_css_element(driver, "#seasons", timeout=self.get_timing_float('season_nav_timeout', 10.0), silent=True)

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

            server_error = self.check_server_error(page_content)
            if server_error:
                raise Exception(f"{server_error}: {url}")

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
            has_malformed_episodes = False
            max_retries = self._season_max_retries

            for idx, season_item in enumerate(season_links):
                season_label, season_url, watched_status, season_type = self.parse_season_item(season_item)

                try:
                    # Per-season auth check: catch session expiry before navigating
                    if not self._has_auth_cookies(driver):
                        if not self.is_logged_in(driver):
                            logger.warning(f"Session expired before season {season_label} of {url} — re-authenticating")
                            if not (self._apply_cookies_to_driver(driver) and self.is_logged_in(driver)):
                                self.login(driver)

                    
                    episodes = []
                    season_failed = True
                    for attempt in range(max_retries):
                        if not self._is_driver_alive(driver):
                            logger.error(f"Driver died during season {season_label} retries — aborting series")
                            break
                        try:
                            driver.get(season_url)
                            # Wait for the season page to finish loading before parsing.
                            self._wait_for_page_ready(driver, timeout=self.get_timing_float('season_page_ready_timeout', 5.0))
                            # Use a fixed timeout for the episodes table.
                            silent = attempt < max_retries - 1
                            if self.wait_for_css_element(driver, "table.episodes", timeout=self.get_timing_float('episodes_table_timeout', 8.0), silent=silent):
                                season_html = driver.page_source
                                episodes, malformed = self.scrape_episodes_from_html(season_html)
                                if malformed > 0:
                                    logger.warning(f"{malformed} malformed episode row(s) in season {season_label} of {url}")
                                    print(f"  ⚠ {malformed} malformed episode row(s) in season {season_label} — marking series for retry")
                                    has_malformed_episodes = True
                                season_failed = False
                                break
                            else:
                                if attempt < max_retries - 1:
                                    print(f"⚠ Retrying season {season_label} (attempt {attempt + 2}/{max_retries})")
                        except Exception as inner_e:
                            if attempt < max_retries - 1:
                                print(f"⚠ Error loading season {season_label}, retrying (attempt {attempt + 2}/{max_retries}): {inner_e}")
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

            # Ensure title exists or use fallback
            if not title_value:
                title_value = series_hint.get('title') if series_hint else None
            if not title_value:
                # Ultimate fallback: extract from URL
                title_value = url.split('/')[-1] or 'Unknown Series'
            
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

            # Safe calculation of episode counts (handle edge cases)
            total_eps = max(0, total_eps)  # Never negative
            total_watched = min(total_watched, total_eps)  # Watched can't exceed total
            unwatched = max(0, total_eps - total_watched)  # Never negative

            result = {
                "title": str(title_value),  # Ensure string
                "link": link,
                "url": url,
                "total_seasons": len(seasons_data),
                "total_episodes": total_eps,
                "watched_episodes": total_watched,
                "unwatched_episodes": unwatched,
                "seasons": seasons_data
            }
            if has_malformed_episodes:
                result["_has_malformed_episodes"] = True
            return result
        except Exception as e:
            raise Exception(f"Series processing failed for {url}: {str(e)}")
    
    # ==================== SCRAPING MODES ====================
    
    def scrape_series_list(self):
        """Scrape all series in sequential or parallel mode."""
        try:
            # Brief initial delay
            time.sleep(self.get_timing_float('initial_delay', 0.3))
            
            all_series = self.get_all_series()
            self.all_discovered_series = all_series

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
        self._historical_avg = self._load_scrape_timing()
        start_time = time.time()
        try:
            for idx, series in enumerate(all_series, 1):
                # Check for pause request
                if self.is_pause_requested():
                    break

                # Recover dead driver before attempting the next series
                if not self._is_driver_alive():
                    logger.warning("Main driver died — restarting for sequential scrape")
                    print("  ⚠ Browser crashed — restarting...")
                    try:
                        self.close()
                    except Exception:
                        pass
                    try:
                        self.setup_driver()
                        self.login()
                        logger.info("Main driver restarted successfully")
                        print("  ✓ Browser restarted")
                    except Exception as restart_err:
                        logger.error(f"Failed to restart main driver: {restart_err}")
                        print(f"  ✗ Failed to restart browser: {restart_err}")
                        break

                try:
                    # Calculate progress and ETA
                    elapsed = time.time() - start_time
                    eta_mins = self._compute_eta_mins(idx - 1, len(all_series), elapsed, self._historical_avg)
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
                            print(f"[{idx}/{len(all_series)}] [{bar}] {progress_pct}% | ETA: {eta_mins}m | Fallback | ⚠ {result['title']}{season_info}: No episodes")
                        else:
                            result['empty'] = False
                            print(f"[{idx}/{len(all_series)}] [{bar}] {progress_pct}% | ETA: {eta_mins}m | Fallback | ✓ {result['title']}{season_info}: {result['watched_episodes']}/{result['total_episodes']} watched")
                        self.series_data.append(result)
                        # Track series with parsing issues for rescrape
                        if result.get('_has_malformed_episodes'):
                            self.failed_links.append(series)
                        # Save checkpoint periodically
                        self.completed_links.add(series.get('link'))
                        if idx % CHECKPOINT_EVERY == 0:
                            self.save_checkpoint()
                    else:
                        print(f"[{idx}/{len(all_series)}] [{bar}] {progress_pct}% | ETA: {eta_mins}m | Fallback | ⚠ {series['title']}: Skipped (no data)")
                        self.failed_links.append(series)
                except Exception as e:
                    print(f"  ⚠ Error processing {series['title']}: {str(e)}")
                    self.failed_links.append(series)
                    continue
        except (KeyboardInterrupt, SystemExit):
            print(f"\n⚠ Ctrl+C — saving {len(self.series_data)}/{len(all_series)} scraped series...")
            raise
        
        # Check if pause was requested — raise to trigger checkpoint save in run()
        if self.is_pause_requested():
            self.clear_pause_request()
            print(f"\n⏸ Scraping paused by user")
            print(f"✓ Progress saved: {len(self.series_data)}/{len(all_series)} series scraped")
            print(f"→ Resume later with checkpoint option\n")
            raise ScrapingPaused(f"{len(self.series_data)}/{len(all_series)} series scraped")

        # Save failed series for later retry (sequential mode)
        if self.failed_links:
            self.save_failed_series()
            print(f"\n⚠ {len(self.failed_links)} series failed. Saved to .failed_series.json for retry.")

        # Final summary
        total_time = time.time() - start_time
        total_mins = int(total_time / 60)
        total_secs = int(total_time % 60)
        self._save_scrape_timing(total_time, len(all_series))
        print(f"\n✓ Completed in {total_mins}m {total_secs}s", flush=True)
    
    def _scrape_series_parallel(self, all_series, worker_cap=None):
        """Parallel scraping with shared work queue and per-worker Firefox instances."""
        # Clear leftover pause file
        self.clear_pause_request()
        
        all_series = self._filter_completed(all_series)
        if all_series is None:
            return

        self._historical_avg = self._load_scrape_timing()
        start_time = time.time()
        completed = 0
        failed = 0
        lock = threading.Lock()
        stop_event = threading.Event()

        # Filter utility pages (case-insensitive)
        filtered_series = [s for s in all_series if not any(keyword in s.get('title', '').lower().strip() for keyword in _UTILITY_PAGES)]
        total_series = len(filtered_series)

        max_workers_allowed = worker_cap if worker_cap is not None else MAX_WORKERS
        # Scale workers: ~1 per 15 series, minimum 1, capped at max_workers_allowed
        worker_count = min(max_workers_allowed, max(1, total_series // 15))
        
        # Shared work queue
        work_queue = queue.Queue()
        for item in filtered_series:
            work_queue.put(item)
        
        print(f"→ {total_series} series queued for {worker_count} workers (shared work queue)")

        def progress_line(done, total, title, watched=None, episode_total=None, empty=False, error=None, worker_id=None, season_labels=None):
            elapsed = time.time() - start_time
            processed = done + failed
            eta_mins = self._compute_eta_mins(processed, total, elapsed, self._historical_avg)
            pct = int((processed / total) * 100)
            bar_len = 30
            filled = int(bar_len * processed / total)
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
            driver = None
            try:
                driver = self._create_worker_driver(worker_id)
            except Exception as e:
                logger.error(f"Worker #{worker_id}: failed to create driver: {e}", exc_info=True)
                print(f"  ✗ Worker #{worker_id}: Failed to create browser: {str(e)[:80]}")
                return
            success_delay = self.get_timing_float('success_delay', 0.15)
            backoff_base = self.get_timing_float('error_backoff_base', 1.0)
            backoff_max = self.get_timing_float('error_backoff_max', 8.0)
            health_every = self.get_timing_int('health_check_every', 10)
            restart_threshold = self.get_timing_int('error_restart_threshold', 5)

            error_streak = 0
            tasks_since_check = 0
            
            # Share auth cookies from main driver
            authenticated = False
            max_auth_retries = MAX_AUTH_RETRIES
            auth_page_delay = self.get_timing_float('worker_auth_page_delay', 1.0)
            auth_retry_delay = self.get_timing_float('worker_auth_retry_delay', 1.0)
            for attempt in range(max_auth_retries):
                try:
                    if self._apply_cookies_to_driver(driver) and self.is_logged_in(driver):
                        authenticated = True
                        break
                    else:
                        # Cookie sharing failed, fall back to full login
                        self.login(driver)
                        driver.get(self.get_site_url())
                        time.sleep(auth_page_delay)
                        if self.is_logged_in(driver):
                            authenticated = True
                            break
                        else:
                            print(f"  ⚠ Worker #{worker_id}: Login verification failed (try {attempt + 1}/{max_auth_retries})")
                            time.sleep(auth_retry_delay)
                except Exception as e:
                    print(f"  ⚠ Worker #{worker_id}: Auth failed - {str(e)[:80]}")
                    time.sleep(auth_retry_delay)
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
                            # Track series with parsing issues for rescrape
                            if result.get('_has_malformed_episodes'):
                                self.failed_links.append(series)
                            self.completed_links.add(series.get('link'))
                            completed += 1
                            if completed % CHECKPOINT_EVERY == 0:
                                self.save_checkpoint()
                            watched = result.get('watched_episodes', 0)
                            total_eps = result.get('total_episodes', 0)
                            progress_line(completed, total_series, result.get('title', 'Series'), watched=watched, episode_total=total_eps, empty=empty, worker_id=worker_id, season_labels=[s.get('season', '?') for s in result.get('seasons', [])])
                            error_streak = 0
                            tasks_since_check += 1
                            
                        else:
                            failed += 1
                            self.failed_links.append(series)
                            progress_line(completed, total_series, series.get('title', 'Series'), error='skipped', worker_id=worker_id)
                            error_streak += 1
                except Exception as e:
                    if not self._is_driver_alive(driver):
                        # Driver was killed externally — restart it and re-queue item
                        logger.warning(f"Worker #{worker_id}: Driver died during scrape — restarting")
                        print(f"  ⚠ Worker #{worker_id}: Browser crashed — restarting...")
                        try:
                            work_queue.put(series)
                        except Exception:
                            pass
                        driver, ok = self._restart_worker_driver(worker_id, driver)
                        if not ok:
                            break
                        error_streak = 0
                        print(f"  ✓ Worker #{worker_id}: Browser restarted")
                        continue
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
                        time.sleep(self.get_timing_float('error_backoff_fallback', 0.5))

                do_health_check = (tasks_since_check >= health_every) or (error_streak >= 3)
                if do_health_check:
                    tasks_since_check = 0
                    error_streak, alive = self._worker_health_check(worker_id, driver, error_streak)
                    if not alive:
                        break

                if error_streak >= restart_threshold:
                    driver, ok = self._restart_worker_driver(worker_id, driver)
                    if not ok:
                        break
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
                # Stagger worker startup so they don't all hit the site at once
                if worker_id < worker_count:
                    time.sleep(2.0)

            # Wait for all workers to complete
            for f in as_completed(futures):
                pass
        except (KeyboardInterrupt, SystemExit):
            print(f"\n⚠ Ctrl+C — saving {len(self.series_data)}/{total_series} scraped series...")
            raise
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
            self.clear_pause_request()
            print(f"\n⏸ Scraping paused by user")
            print(f"✓ Progress saved: {len(self.series_data)}/{total_series} series scraped")
            print(f"→ Resume later with checkpoint option\n")
            raise ScrapingPaused(f"{len(self.series_data)}/{total_series} series scraped")
        
        # Save failed series for later retry
        if self.failed_links:
            self.save_failed_series()
            print(f"\n⚠ {len(self.failed_links)} series failed. Use 'Retry failed series' (option 6) to rescrape with a fresh login.")

        total_time = time.time() - start_time
        total_mins = int(total_time / 60)
        total_secs = int(total_time % 60)
        self._save_scrape_timing(total_time, total_series)
        if failed:
            print(f"\n✓ Completed in {total_mins}m {total_secs}s ({failed} failed)", flush=True)
        else:
            print(f"\n✓ Completed in {total_mins}m {total_secs}s", flush=True)

    def _create_worker_driver(self, worker_id=None):
        """Create a new worker WebDriver and track its PID.
        
        Args:
            worker_id: Optional worker ID for tracking
            
        Returns:
            WebDriver instance or None if creation fails
            
        Raises:
            Exception: If driver creation fails after retries
        """
        driver = None
        try:
            firefox_options = self._build_firefox_options()
            service = self._build_firefox_service()
            
            driver = webdriver.Firefox(service=service, options=firefox_options)
            driver.set_page_load_timeout(self.get_timing_float('page_load_timeout', 20.0))
            
            # Install uBlock Origin for ad-blocking
            try:
                self._install_ublock(driver)
            except Exception as e:
                logger.debug(f"Failed to install uBlock Origin: {e}")
                # Continue anyway, uBlock is optional
            
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
                    if not pid_saved and attempt < 2:
                        time.sleep(self.get_timing_float('worker_service_init_delay', 0.1))
                
                if not pid_saved:
                    logger.debug(f"Worker #{worker_id}: Could not track PID")
            
            return driver
        except Exception as e:
            # Clean up any partial driver/service created before error
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
            elif hasattr(service, 'process') and service.process:
                try:
                    service.stop()
                except Exception:
                    pass
            raise Exception(f"Failed to create worker driver: {str(e)}")
    
    def _restart_worker_driver(self, worker_id, old_driver):
        """Restart a worker's browser and re-authenticate via cookies or login.

        Args:
            worker_id: Worker number for logging.
            old_driver: The crashed/stale driver to quit.

        Returns:
            tuple: (new_driver, success) — new_driver is None on failure.
        """
        try:
            old_driver.quit()
        except Exception:
            pass
        try:
            driver = self._create_worker_driver(worker_id)
            if not self._authenticate_driver(driver, label=f"Worker #{worker_id}"):
                logger.error(f"Worker #{worker_id}: Failed to authenticate after restart")
                try:
                    driver.quit()
                except Exception:
                    pass
                return None, False
            return driver, True
        except Exception as e:
            logger.error(f"Worker #{worker_id}: Failed to restart driver: {e}", exc_info=True)
            return None, False

    def _worker_health_check(self, worker_id, driver, error_streak):
        """Perform a health check on a worker driver and re-authenticate if needed.

        Args:
            worker_id: Worker number for logging.
            driver: The worker's WebDriver.
            error_streak: Current consecutive error count.

        Returns:
            tuple: (error_streak, driver_alive) — driver_alive is False if driver is dead.
        """
        if not self._is_driver_alive(driver):
            return error_streak, False

        needs_reauth = False
        if error_streak >= 3:
            try:
                if not self.is_logged_in(driver):
                    driver.get(self.get_site_url())
                    if not self.is_logged_in(driver):
                        needs_reauth = True
            except Exception:
                needs_reauth = True
        else:
            if not self._has_auth_cookies(driver):
                try:
                    if not self.is_logged_in(driver):
                        needs_reauth = True
                except Exception:
                    needs_reauth = True

        if needs_reauth:
            label = f"Worker #{worker_id}"
            if self._authenticate_driver(driver, label=label):
                error_streak = 0
            else:
                error_streak += 1
        return error_streak, True

    def _authenticate_driver(self, driver, label=None, max_attempts=3):
        """Authenticate a worker driver via cookies or full login.
        
        Tries cookie-based auth first, falls back to full login.
        Retries up to max_attempts times.
        
        Args:
            driver: WebDriver instance to authenticate
            label: Label for log messages (e.g. 'Worker #3')
            max_attempts: Number of auth attempts before giving up
            
        Returns:
            bool: True if authenticated successfully
        """
        label = label or 'driver'
        for attempt in range(max_attempts):
            retry_delay = self.get_timing_float('worker_auth_retry_delay', 1.0)
            try:
                if self._apply_cookies_to_driver(driver) and self.is_logged_in(driver):
                    logger.debug(f"{label}: authenticated via cookies")
                    return True
                else:
                    self.login(driver)
                    if self.is_logged_in(driver):
                        logger.debug(f"{label}: authenticated via full login")
                        return True
                    else:
                        logger.warning(f"{label}: login verification failed (attempt {attempt + 1}/{max_attempts})")
                        print(f"  ⚠ {label}: Login verification failed (try {attempt + 1}/{max_attempts})")
                        time.sleep(retry_delay)
            except Exception as e:
                logger.warning(f"{label}: auth exception (attempt {attempt + 1}/{max_attempts}): {e}")
                print(f"  ⚠ {label}: Auth failed - {str(e)[:80]}")
                time.sleep(retry_delay)

        logger.error(f"{label}: failed to authenticate after {max_attempts} attempts")
        return False

    def _apply_cookies_to_driver(self, driver):
        """Copy auth cookies to a worker driver (thread-safe snapshot)."""
        with self._worker_lock:
            cookies_snapshot = list(self.auth_cookies)
        if not cookies_snapshot:
            return False
        try:
            driver.get(self.get_site_url())
            self._wait_for_page_ready(driver, timeout=self.get_timing_float('cookie_apply_page_ready_timeout', 5.0))
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
                except WebDriverException:
                    continue
            driver.refresh()
            return True
        except (WebDriverException, TimeoutException):
            return False
    
    # ==================== DATA MANAGEMENT ====================
    
    def get_series_slug_from_url(self, url):
        """Extract series slug from full URL or relative path.
        
        Handles both /serie/slug and full URLs with host.
        
        Returns:
            str: Series slug (e.g., 'breaking-bad') or 'unknown' on failure
        """
        try:
            if url.startswith('http'):
                path = urlparse(url).path
            else:
                path = url
            parts = path.split('/')
            if 'serie' in parts:
                idx = parts.index('serie')
                if idx + 1 < len(parts) and parts[idx + 1]:
                    return parts[idx + 1]
            return 'unknown'
        except Exception:
            return 'unknown'

    def load_existing_slugs(self):
        """Load existing series slugs from the index (for new-only filtering)."""
        existing = set()
        try:
            if os.path.exists(SERIES_INDEX_FILE):
                with open(SERIES_INDEX_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    for item in data or []:
                        url = item.get('url', '') or item.get('link', '')
                        if url:
                            existing.add(self.get_series_slug_from_url(url))
                elif isinstance(data, dict):
                    for v in data.values():
                        url = v.get('url', '') or v.get('link', '')
                        if url:
                            existing.add(self.get_series_slug_from_url(url))
        except Exception:
            pass
        existing.discard('unknown')
        return existing
    
    def scrape_new_series_only(self):
        """Scrape only series not yet in the index."""
        time.sleep(self.get_timing_float('initial_delay', 0.3))
        
        all_series = self.get_all_series()
        self.all_discovered_series = all_series
        existing_slugs = self.load_existing_slugs()

        new_series_list = [s for s in all_series
                          if self.get_series_slug_from_url(s.get('link', '')) not in existing_slugs]

        print(f"→ New series to scrape: {len(new_series_list)} (out of {len(all_series)})")
        self.series_data = []
        if not new_series_list:
            print("✓ No new series detected — skipping scraper spin-up")
            return

        # Run sequential for small delta set
        self._scrape_series_sequential(new_series_list)
    
    def scrape_retry_failed(self):
        """Retry previously failed series in sequential mode with higher retry count."""
        failed_list = self.load_failed_series()
        if not failed_list:
            print("✓ No failed series found")
            return
    
        print(f"✓ Found {len(failed_list)} failed series from last run")
        print("→ Starting retry in sequential mode (for reliability)...")
        self.series_data = []
        # Use higher retry count for retry mode
        self._season_max_retries = self.get_timing_int('max_retries_retry', 5)
    
        self._scrape_series_sequential(failed_list)
    # ==================== MAIN RUN METHODS ====================
    
    def scrape_single_series(self, url):
        """Scrape exactly one series by URL."""
        time.sleep(self.get_timing_float('initial_delay', 0.3))
        
        main_url = self.normalize_to_series_url(url)
        print(f"→ Scraping single series (normalized): {main_url}")
        result = self.process_series_page(main_url)
        self.series_data = [result]
        return result
    
    def scrape_multiple_series(self, urls):
        """Scrape multiple series from a URL list."""
        time.sleep(self.get_timing_float('initial_delay', 0.3))
        
        # Normalize all URLs to main series page
        series_list = []
        for url in urls:
            main_url = self.normalize_to_series_url(url)
            m = _SERIE_PATH_RE.search(main_url)
            link_path = m.group(1) if m else main_url
            series_list.append({'title': main_url.split('/')[-1], 'link': link_path, 'url': main_url})
        self.series_data = []
        if self._use_parallel and len(series_list) > 1:
            print(f"→ Scraping {len(urls)} series from URL list (parallel mode)...")
            self._scrape_series_parallel(series_list)
        else:
            print(f"→ Scraping {len(urls)} series from URL list (sequential mode)...")
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
            
            # Save checkpoint with data so caller can confirm save before clearing
            self.save_checkpoint(include_data=True)
            # Only clear failed series file if no failures remain
            if not self.failed_links:
                self.clear_failed_series()
            else:
                self.save_failed_series()
        except ScrapingPaused:
            # Pause requested — save full checkpoint with data for resume
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
            # Don't re-raise — let caller process whatever series_data we collected
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
