# bs.to Series Scraper

> Track your watched TV series from [bs.to](https://bs.to) with a local JSON database. Detects new episodes, season additions, and watch-status changes.

![Python](https://img.shields.io/badge/python-3.8%2B-blue)
![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey)
![License](https://img.shields.io/badge/license-MIT-green)

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Menu Options](#menu-options)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Data Structure](#data-structure)
- [Parallel Scraping](#parallel-scraping)
- [Checkpoint & Resume](#checkpoint--resume)
- [Troubleshooting](#troubleshooting)
- [Privacy & Legal](#privacy--legal)

---

## Features

- Scrapes all your series from bs.to (title, seasons, episodes, watch status)
- Maintains a local JSON database — no cloud, no accounts
- Detects **new series**, **new episodes**, and **watch-status changes**
- Interactive confirmation before any changes are saved
- **Sequential mode** — reliable, ~10-15s per series
- **Parallel mode** — up to 16 concurrent Firefox workers, 15+ series/min
- Checkpoint resume — safe to interrupt and continue later
- Automatic retry for failed series
- Full stats report (completion %, ongoing vs. finished, episode counts)

---

## Requirements

- Python 3.8+
- Firefox browser (geckodriver auto-detected from PATH or `geckodriver.exe` in project root)
- See `requirements.txt` for Python dependencies

---

## Installation

```bash
git clone https://github.com/yourusername/bs.to-scraper.git
cd bs.to-scraper
pip install -r requirements.txt
```

**Set up credentials:**

```bash
# Windows
copy .env.example .env

# Linux/macOS
cp .env.example .env
```

Edit `.env`:

```
BS_USERNAME=your_username
BS_PASSWORD=your_password
```

> ⚠️ Never commit `.env` — it's in `.gitignore` by default.

---

## Usage

```bash
python main.py
```

---

## Menu Options

```
  1. Scrape series from bs.to     — full scrape, sequential or parallel
  2. Scrape only NEW series        — only series not yet in your database
  3. Add single series by URL      — one URL at a time
  4. Generate full report          — stats, completion %, categories
  5. Batch add from file           — series_urls.txt, one URL per line
  6. Retry failed series           — re-runs any series that errored last time
  7. Pause current scraping        — creates pause flag in another terminal
  8. Show active workers           — lists running geckodriver PIDs
  9. Exit
```

### Option 1 — Scrape All

Fetches the full list from `/andere-serien`, asks Sequential or Parallel, shows a live progress bar, then presents all detected changes (paginated) for your approval before saving.

```
[47/150] [█████████░░░░░░░░░░░░░░░░░░░] 31% | ETA: 14m | ✓ Breaking Bad [S1,S2,S3]: 45/62 watched
```

### Option 2 — Scrape New Only

Compares live series list against your database slugs. Only scrapes series not already present — ideal for weekly updates.

### Option 5 — Batch Add

Create `series_urls.txt` with one URL per line:

```
https://bs.to/serie/Breaking-Bad
https://bs.to/serie/Game-of-Thrones
```

Then run option 5.

### Option 7 — Pause

Run option 7 in a second terminal while a scrape is running. The script creates the `.pause_scraping` flag automatically — the running scraper detects it at the next series boundary, saves a checkpoint, and stops cleanly.

---

## Architecture

```
main.py
  ├── setup_driver()       → Firefox + uBlock Origin
  ├── login()              → JS form injection (no Selenium click chains)
  ├── get_all_series()     → parses /andere-serien with BeautifulSoup
  ├── scrape_series() ─┬─ Sequential: one series at a time, checkpoint every 10
  │                    └─ Parallel:   ThreadPoolExecutor + shared queue.Queue
  │                                   cookie sharing → login fallback per worker
  │                                   health checks + auto-restart on errors
  ├── detect_changes()     → diffs old vs. new index (new series, eps, status)
  ├── merge_series_data()  → user-confirmed watched/unwatched merging
  └── _atomic_write_json() → temp file + os.replace (crash-safe)
```

**Hidden runtime files** (in `data/`):

| File                      | Purpose                                          |
| ------------------------- | ------------------------------------------------ |
| `series_index.json`       | Main series database                             |
| `.scrape_checkpoint.json` | Completed slugs + full scraped data (for resume) |
| `.failed_series.json`     | Series that errored (for option 6 retry)         |
| `.worker_pids.json`       | Geckodriver PIDs (cleaned up on exit)            |
| `.pause_scraping`         | Pause flag file (created by option 7)            |

---

## Configuration

All scraping behaviour is driven by `config/selectors_config.json` — no hardcoded CSS or timeouts in the Python code.

**Key sections:**

| Section                 | Controls                                           |
| ----------------------- | -------------------------------------------------- |
| `selectors.login`       | Login form field names and submit button           |
| `selectors.series_list` | Series list page URL and link pattern              |
| `selectors.series_page` | Title tag, season link selector, watched CSS class |
| `selectors.episodes`    | Episode table/row selectors, watched row class     |
| `timing`                | All page timeouts, retry counts, worker delays     |

If bs.to changes their HTML, update the selector values in this file — no code changes needed.

**Key timing values:**

```json
{
  "timing": {
    "page_ready_timeout": 10,
    "max_retries_season": 3,
    "max_retries_retry": 5,
    "health_check_every": 15,
    "error_restart_threshold": 8
  }
}
```

---

## Data Structure

`data/series_index.json` is a JSON array:

```json
[
  {
    "title": "Breaking Bad",
    "link": "/serie/Breaking-Bad",
    "url": "https://bs.to/serie/Breaking-Bad",
    "total_seasons": 5,
    "total_episodes": 62,
    "watched_episodes": 45,
    "unwatched_episodes": 17,
    "added_date": "2024-01-15T10:30:00",
    "last_updated": "2024-03-25T14:22:15",
    "seasons": [
      {
        "season": "Staffel 1",
        "url": "https://bs.to/serie/Breaking-Bad/0/de",
        "watched_episodes": 7,
        "total_episodes": 7,
        "episodes": [{ "number": "1", "title": "Pilot", "watched": true }]
      }
    ]
  }
]
```

---

## Parallel Scraping

Default: up to **16 concurrent Firefox workers** with a shared `queue.Queue` — faster workers automatically pull more tasks.

**Auth flow:** Main driver logs in once → auth cookies shared to all workers → per-worker login fallback if cookies fail.

**Self-healing:** Each worker runs a health check every 15 tasks (configurable). After 8 consecutive errors it restarts its own browser.

**Configure worker count:**

```bash
# Windows
$env:BS_MAX_WORKERS = "8"
python main.py

# Linux/macOS
BS_MAX_WORKERS=8 python main.py
```

**Use sequential mode** (option 1 → choose mode 1) when:

- Network is unreliable
- System is under heavy load
- Debugging scraping issues

---

## Checkpoint & Resume

Every 10 series, progress is saved to `data/.scrape_checkpoint.json` (completed slugs + all scraped data). On Ctrl+C or crash, a final checkpoint is written immediately.

On the next run, the menu offers to resume from that checkpoint — already-completed series are skipped automatically.

---

## Files & Directories

```
bs.to-scraper/
├── main.py                     # Entry point + interactive menu
├── config/
│   ├── config.py               # Loads .env, sets paths
│   └── selectors_config.json   # All CSS selectors and timing
├── src/
│   ├── scraper.py              # Selenium/BeautifulSoup scraping engine
│   └── index_manager.py        # Change detection, merging, saving
├── data/
│   └── series_index.json       # Your series database (gitignored)
├── logs/
│   └── bs_to_backup.log        # Rotating log (10 MB max, 5 backups)
├── addons/
│   └── ublock_origin.xpi       # Ad blocker for faster page loads
├── .env.example                # Credentials template
├── .gitignore
└── requirements.txt
```

---

## Troubleshooting

**Login failed**

- Confirm credentials in `.env` are correct
- Log in manually at bs.to to check for captcha or 2FA
- Sessions can expire — just try again

**Series not found**

- bs.to may have removed the series
- It's saved to `.failed_series.json` automatically
- Use option 6 to retry with a higher timeout (5 attempts vs. 3)

**Parallel mode crashes**

- Switch to Sequential (option 1 → mode 1)
- Or reduce worker count: `$env:BS_MAX_WORKERS = "4"`

**Scraping stops working entirely**

- bs.to likely changed their HTML structure
- Open browser dev tools (F12), inspect the broken element
- Update the matching selector in `config/selectors_config.json`

**Corrupted database**

- Atomic writes protect against mid-write crashes
- Backups are kept: `series_index.json.bak1`, `.bak2`, `.bak3`
- Restore a backup or delete the file and re-scrape to rebuild

---

## Privacy & Legal

- **Personal use only** — verify bs.to's Terms of Service before use
- `data/series_index.json` contains your watch history — keep it private
- Never commit `.env` or `series_index.json` to a public repository
- Nothing is uploaded anywhere — all data stays on your machine

---

## Contributing

Bug reports and pull requests welcome. Please include:

- The relevant error from `logs/bs_to_backup.log`
- Your OS, Python version, and Firefox version

---

## License

MIT License

- Scrapes TV series data from bs.to (title, seasons, episodes, watch status)
- Maintains a local JSON database of your series
- Detects new series, new episodes, and watch status changes
- Supports both **sequential** (reliable) and **parallel** (fast) scraping
- Resumes interrupted scrapes from checkpoints
- Manages failed series for retry
- Generates detailed statistics and completion reports

🚀 **Performance:**

- **Sequential mode:** ~10-15 seconds per series (most reliable)
- **Parallel mode:** Multiple workers, can scrape 15+ series concurrently
- Automatic retry with configurable timeouts
- Health checks and auto-restart for worker processes

🔒 **Reliability:**

- Atomic file writes (no corruption on crashes)
- Checkpoint resume system
- Handles interrupts gracefully (Ctrl+C safe)
- Detailed logging of all operations
- Failed series tracking for later retry

## Requirements

- **Python 3.8+**
- **Firefox browser** (with geckodriver auto-detection)
- **Dependencies:** beautifulsoup4, selenium, python-dotenv

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/bs.to-scraper.git
cd bs.to-scraper
```

### 2. Create Virtual Environment (Optional but Recommended)

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Credentials

```bash
# Copy the example file
cp .env.example .env

# Edit .env and add your bs.to username and password
# BS_USERNAME=your_username
# BS_PASSWORD=your_password
```

⚠️ **IMPORTANT:** Never commit `.env` to version control. It's included in `.gitignore` for your safety.

## Usage

### Run the Main Menu

```bash
python main.py
```

You'll see an interactive menu:

```
===========================================================
  BS.TO SERIES SCRAPER & INDEX MANAGER
===========================================================

Options:
  1. Scrape series from bs.to (requires login)
  2. Scrape only NEW series (faster)
  3. Add single series by URL
  4. Generate full report
  5. Batch add series from text file
  6. Retry failed series from last run
  7. Pause current scraping (in another terminal)
  8. Show active workers
  9. Exit
```

### Option 1: Scrape All Series

- Fetches complete list from `/andere-serien`
- Asks: Sequential or Parallel mode?
- Displays progress bar with ETA
- Shows detected changes (paginated)
- Requires confirmation before saving

```
[1/150] [████░░░░░░░░░░░░░░░░░░░░░░] 6% | ETA: 25m | ✓ Breaking Bad [S1,S2,S3]: 45/62 watched
```

### Option 2: Scrape New Series Only

- Faster than full scrape
- Only scrapes series NOT already in your database
- Perfect for weekly updates

### Option 3: Add Single Series by URL

```
Enter series URL: https://bs.to/serie/Breaking-Bad
```

### Option 4: Generate Report

Shows statistics:

- Total series & episodes watched
- Completion percentages
- Most/least completed series
- Distribution by completion range (0-25%, 25-50%, etc.)

### Option 5: Batch Add from File

Add multiple series at once:

1. Create `series_urls.txt`:

```
https://bs.to/serie/Breaking-Bad
https://bs.to/serie/Game-of-Thrones
https://bs.to/serie/The-Office
```

2. Select option 5 and it will scrape all of them

### Option 6: Retry Failed Series

- Automatically saves failed series during scraping
- Uses higher retry count (5 attempts vs. 3)
- Sequential mode for reliability

### Option 7: Pause Scraping

- Run option 7 in a second terminal while a scrape is running
- Script creates the `.pause_scraping` flag file automatically
- Running scraper detects it and pauses gracefully at the next series
- Progress is saved to checkpoint
- Resume later with "Scrape all" → "Resume from checkpoint"

## Architecture

```
main.py
  ├── setup_driver()          → Firefox + uBlock Origin
  ├── login()                 → JS injection of credentials
  ├── get_all_series()        → Scrapes /andere-serien for series links
  ├── scrape_series() ─┬─ Sequential: one series at a time, checkpoint every 10
  │                    └─ Parallel:   ThreadPoolExecutor, shared queue.Queue,
  │                                   cookie sharing, health checks, auto-restart
  ├── detect_changes()        → Diffs old vs new index
  ├── merge_series_data()     → Merges with user confirmation for watch changes
  └── _atomic_write_json()    → temp file + os.replace (crash-safe)
```

**Hidden data files** (in `data/`):

| File                      | Purpose                                           |
| ------------------------- | ------------------------------------------------- |
| `series_index.json`       | Main database of all series                       |
| `.scrape_checkpoint.json` | Resume state (completed slugs + full data so far) |
| `.failed_series.json`     | Series that errored during scrape (for retry)     |
| `.worker_pids.json`       | Geckodriver PIDs (cleanup on exit)                |
| `.pause_scraping`         | Flag file — create it to pause a running scrape   |

## What Happens During Scraping

### Process

1. **Authenticate** → Logs into bs.to using Selenium
2. **Discover** → Fetches all series links from `/andere-serien`
3. **Scrape** → For each series:
   - Extracts title and season list
   - For each season: parses episode table
   - Detects watched status (CSS class `watched`)
4. **Detect Changes** → Compares with existing database:
   - New series (not in database)
   - New episodes (added since last scrape)
   - Watched/unwatched status changes
5. **Confirm** → Shows paginated change report
   - Prompts for approval on watch status changes
6. **Merge** → Combines with existing data
7. **Save** → Writes atomically to `data/series_index.json`

### Change Detection Example

```
======================================================================
  CHANGES DETECTED
======================================================================

[NEW SERIES] (2)
  + Breaking Bad: 45/62 watched
  + The Office: 120/201 watched

[NEW EPISODES] (8)
  + Game of Thrones [Season 5]: 10/10 episodes
  + Stranger Things [Season 2]: 1/9 episodes

[NEWLY WATCHED] (3 episodes)
  [+] The Last of Us [Season 1]: 3/9 episodes

[SITE REPORTS UNWATCHED] (0 episodes)

======================================================================
```

## Data Structure

### Your Database: `data/series_index.json`

```json
[
  {
    "title": "Breaking Bad",
    "link": "/serie/Breaking-Bad",
    "url": "https://bs.to/serie/Breaking-Bad",
    "total_seasons": 5,
    "total_episodes": 62,
    "watched_episodes": 45,
    "unwatched_episodes": 17,
    "added_date": "2024-01-15T10:30:00",
    "last_updated": "2024-03-25T14:22:15",
    "seasons": [
      {
        "season": "Staffel 1",
        "url": "https://bs.to/serie/Breaking-Bad/0/de",
        "watched_episodes": 7,
        "total_episodes": 7,
        "episodes": [
          {"number": "1", "title": "Pilot", "watched": true},
          ...
        ]
      }
    ]
  }
]
```

## Configuration

### Selectors (`config/selectors_config.json`)

The scraper uses **CSS/XPath selectors** to find elements on bs.to. If the website changes, you may need to update these:

```json
{
  "selectors": {
    "series_list": {
      "series_links": {
        "type": "css",
        "value": "a[href*='/serie/']"
      }
    },
    "series_page": {
      "title": {
        "type": "tag",
        "value": "h2"
      },
      "season_selector": {
        "type": "css",
        "value": "#seasons a"
      }
    },
    "episodes": {
      "table": {
        "type": "css",
        "value": "table.episodes"
      }
    }
  }
}
```

### Timing (`config/selectors_config.json`)

```json
{
  "timing": {
    "page_ready_timeout": 10.0,
    "season_page_ready_timeout": 5.0,
    "max_retries_season": 3,
    "success_delay": 0.15,
    "error_backoff_base": 1.0
  }
}
```

## Logs

All operations are logged to `logs/bs_to_backup.log`:

```
2024-03-25 14:22:15,123 - INFO - Loaded index from data/series_index.json (45 entries)
2024-03-25 14:22:16,456 - INFO - Login completed
2024-03-25 14:22:20,789 - INFO - Found 95 unique series
2024-03-25 14:25:30,012 - INFO - Successfully scraped 95 series
2024-03-25 14:25:31,234 - INFO - Detected changes: new_series=2, new_episodes=8
2024-03-25 14:25:35,567 - INFO - Saved 97 series to data/series_index.json
```

## Troubleshooting

### "Login failed"

- Verify credentials in `.env` are correct
- Try logging in manually to bs.to (check for captcha, 2FA)
- Sessions may expire; try again

### "Series not found" errors

- Some series may have been removed from bs.to
- They'll be saved to `.failed_series.json` for retry
- Use option 6 to retry with higher timeouts

### Parallel mode crashes

- Worker processes may fail on your system
- Switch to Sequential mode (option 1 → choose mode 1)
- More reliable, slightly slower

### Selectors out of date

- If scraping stops working: bs.to likely changed their HTML
- Update `config/selectors_config.json` with new CSS selectors
- Check browser dev tools (F12) to inspect element structure

### Database corruption

- Atomic writes protect against crashes
- If `data/series_index.json` is invalid JSON:
  - Delete it (you'll lose history, but scraper will rebuild)
  - Run scrape again to reconstruct database

## Files & Directories

```
save-bs-to/
├── main.py                          # Entry point with interactive menu
├── config/
│   ├── config.py                    # Configuration loader
│   └── selectors_config.json        # CSS/XPath selectors
├── src/
│   ├── scraper.py                   # Selenium/BeautifulSoup scraper
│   └── index_manager.py             # Change detection & merging
├── data/
│   └── series_index.json            # Your series database
├── logs/
│   └── bs_to_backup.log             # Operation log
├── addons/
│   └── ublock_origin.xpi            # Ad blocker (optional)
├── .env.example                     # Credentials template
├── .gitignore                       # Git ignore rules
└── requirements.txt                 # Python dependencies
```

## Advanced: Parallel Scraping

By default, scraper uses **parallel mode** with a **shared work queue** and up to 16 worker Firefox instances. This design provides optimal load balancing:

**How it works:**

- All series queued in shared `queue.Queue()`
- Each worker pulls tasks dynamically (not pre-assigned)
- Faster workers get more tasks → better utilization
- Failed tasks remain in queue for other workers to retry

Configure workers via environment:

```bash
# Windows — use 8 workers instead of 16
$env:BS_MAX_WORKERS = "8"
python main.py

# Linux/macOS
BS_MAX_WORKERS=8 python main.py
```

To force sequential mode, choose option 1 from the main menu and select mode 1 (Sequential).

**Parallel Mode Benefits:**

- **Shared queue:** Dynamic load balancing (faster workers do more)
- **Much faster:** 15+ series per minute vs. 4-6 in sequential
- **Efficient auth:** Single login, cookies shared to all workers
- **Self-healing:** Health checks auto-restart dead workers
- **Automatic backoff:** Exponential backoff on errors for stability

**When to Use Sequential:**

- Unreliable network
- System under load
- Debugging issues
- Older hardware

## Checkpoint & Resume System

If scraping is interrupted:

1. **Checkpoint saved** at `data/.scrape_checkpoint.json`
2. **Progress tracked** (completed series list)
3. **Full data saved** (so far scraped series)
4. On next run: Option to "Resume from checkpoint"

**This means:**

- You won't re-scrape already-completed series
- No progress lost on crash/Ctrl+C
- Safe to interrupt and resume later

## Privacy & Legal

⚠️ **Important:**

- This tool is for **personal use** only
- Verify bs.to's Terms of Service permits scraping
- Your `data/series_index.json` contains watch history
- **Never commit** `.env` or actual series data to public GitHub
- Use a private repository if concerned

## Contributing

Found a bug? Have a suggestion?

- Open an issue on GitHub
- Include error from `logs/bs_to_backup.log`
- Describe your system (OS, Python version, Firefox version)

## License

MIT License - Feel free to use for personal projects.

## FAQ

**Q: Can I use this with other streaming sites?**  
A: Architecture is site-agnostic. Update `selectors_config.json` for another site. Pull requests welcome!

**Q: How often should I scrape?**  
A: Weekly is typical. More frequent = faster detection of changes.

**Q: Does it upload my data anywhere?**  
A: No. Everything stays local on your machine.

**Q: Can I modify watch status?**  
A: No, it's read-only from the website. It only tracks what bs.to reports.

**Q: What if the website blocks me?**  
A: Check if bs.to has IP limits. Try increasing delays in `selectors_config.json`.
