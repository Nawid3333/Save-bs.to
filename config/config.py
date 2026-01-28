import os
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Website configuration
WEBSITE_URL = "https://bs.to"
LOGIN_PAGE = f"{WEBSITE_URL}/login"

# Credentials (store in .env file)
USERNAME = os.getenv("BS_USERNAME", "")
PASSWORD = os.getenv("BS_PASSWORD", "")

# Data storage
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
SERIES_INDEX_FILE = os.path.join(DATA_DIR, "series_index.json")
SELECTORS_FILE = os.path.join(DATA_DIR, "selectors.json")

# Load selectors configuration
CONFIG_DIR = os.path.dirname(__file__)
SELECTORS_CONFIG_FILE = os.path.join(CONFIG_DIR, "selectors_config.json")

def load_selectors_config():
    """Load site-specific selectors configuration from JSON"""
    try:
        with open(SELECTORS_CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠ Warning: Could not load selectors config: {str(e)}")
        return {}

SELECTORS_CONFIG = load_selectors_config()

# Scraping configuration
TIMEOUT = SELECTORS_CONFIG.get("timing", {}).get("timeout", 10)
HEADLESS = True  # Set to False to see browser automation
