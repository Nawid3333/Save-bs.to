import os
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Credentials (store in .env file)
USERNAME = os.getenv("BS_USERNAME", "")
PASSWORD = os.getenv("BS_PASSWORD", "")

# Data storage
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
SERIES_INDEX_FILE = os.path.join(DATA_DIR, "series_index.json")

# Logs directory
LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOGS_DIR, "bs_to_backup.log")

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
HEADLESS = True  # Set to False to see browser automation
