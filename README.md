# BS.TO Series Scraper & Index Manager

A Python tool to automatically scrape your watched TV series from the bs.to website, maintain a local index, and track which series you've watched.

## Features

✨ **High-Performance Scraping**

- **Parallel processing** with up to 24 concurrent workers
- Cookie-based authentication for worker threads
- **~10-15 minutes** to scrape 10,000+ series
- Automatic login to your bs.to account
- Extract watched status for each episode across all seasons
- Handles series with multiple seasons including specials (Season 0)

📊 **Smart Index Management**

- Incremental updates (preserves existing watched status)
- Detects and reports changes before saving
- User approval workflow for all changes
- Filters empty series (no episodes) from statistics
- Track watched/unwatched status per episode
- View statistics and generate reports

🔍 **Interactive CLI**

- Browse watched and unwatched series
- View detailed statistics with empty series count
- Generate comprehensive JSON reports
- Change detection summary with emoji indicators

## Installation

### Prerequisites

- Python 3.7+
- Firefox browser (for Selenium automation)
- Geckodriver (must be on PATH or in project root)
  - Download from: https://github.com/mozilla/geckodriver/releases
  - Windows: Place `geckodriver.exe` in project folder or add to PATH
- Modern CPU recommended for parallel scraping (6+ cores ideal)
- 4GB+ RAM (8GB+ recommended for 24 workers)

### Setup

1. **Clone or download the project**

   ```bash
   cd "Save bs.to"
   ```

2. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

3. **Configure credentials**
   - Copy `.env.example` to `.env`
   - Add your bs.to username and password (keep `.env` out of git):
     ```
     BS_USERNAME=your_username
     BS_PASSWORD=your_password
     ```

## Usage

### Run the Application

```bash
python main.py
```

### Menu Options

1. **Scrape series from bs.to** - Logs in and scrapes all your series
2. **Scrape only NEW series** - Scrapes only series not yet in the index (faster)
3. **Add single series by URL** - Manually add one series
4. **Generate full report** - Creates a detailed JSON report
5. **Show series with progress** - Displays all series with completion percentages
6. **Batch add series from text file** - Add multiple series from `series_urls.txt`
7. **Retry failed series** - Retry series that failed during last scrape
8. **Pause current scraping** - Gracefully pause an ongoing scrape (signal from menu)
9. **Exit** - Close the application

### Pausing Scrapes

Ctrl+C is unreliable for stopping parallel scrapes. Use the pause mechanism instead:

**Option A: From the same terminal (multi-terminal setup)**

1. **Terminal 1**: Run `python main.py` → select option 1 (scraping starts)
2. **Terminal 2**: Run `python main.py` → select option 8 (sends pause signal)
3. **Terminal 1**: Scraping pauses gracefully within ~2 seconds

**Option B: Manual pause (any terminal)**

```powershell
New-Item -Path "data\.pause_scraping" -Force
```

The scraping job will detect this file and pause at the next checkpoint.

**Note**: Progress is saved to checkpoint. Resume with the checkpoint option in the main menu.

## Output Files

All data is stored in the `data/` directory (git-ignored):

- `series_index.json` - Complete index of all series with episodes and watched status
- `watched_series.json` - Legacy watched status tracking (deprecated)
- `series_report.json` - Generated reports with metadata
- `selectors.json` - Approved HTML selectors for scraping

## Project Structure

```
Save bs.to/
├── main.py                 # Main application entry point
├── requirements.txt        # Python dependencies
├── README.md              # This file
├── .env.example           # Template for credentials
├── .env                   # Your credentials (git-ignored)
├── config/
│   └── config.py          # Configuration settings
├── src/
│   ├── scraper.py         # Web scraper with Selenium
│   └── index_manager.py   # Index and data management
└── data/                  # Output data files
    ├── series_index.json
    ├── watched_series.json
    └── series_report.json
```

## How It Works

1. **Authentication**: Main driver logs into bs.to and captures session cookies
2. **Series Discovery**: Scrapes all series links from `/andere-serien`
3. **Parallel Processing**: Spawns worker threads (default: 24) with shared cookies
4. **Episode Scraping**: Each worker:
   - Navigates to assigned series
   - Extracts all seasons (including Season 0 specials)
   - Detects watched status via HTML row `class="watched"` attribute
   - Aggregates watched/total counts per season
5. **Change Detection**: Compares new data with existing index
6. **User Approval**: Shows change summary before saving
7. **Data Merge**: Preserves existing watched status, adds new episodes as unwatched
8. **Statistics**: Excludes empty series (0 episodes) from counts

### Performance Tuning

Adjust workers in `src/scraper.py` line 20:

```python
MAX_WORKERS = 24  # Increase for faster scraping (requires more RAM/CPU)
```

- **Low-end systems**: 4-6 workers
- **Mid-range (4-8 cores)**: 8-12 workers
- **High-end (8+ cores, 32GB+ RAM)**: 16-24 workers

## Notes

⚠️ **Important Security Notes**

- Never commit `.env` file to version control (already in `.gitignore`)
- Keep your credentials secure
- The scraper respects website terms of service
- With parallel mode, respect rate limits: don't run back-to-back scrapes

🚀 **Performance Tips**

- Headless mode is enabled by default (faster)
- First run requires selector approval (title extraction)
- Subsequent runs reuse approved selectors automatically
- Series with 0 episodes are marked `"empty": true` and excluded from stats
- Incremental updates preserve watched status

🔧 **Customization**

- Adjust `MAX_WORKERS` in `src/scraper.py` for your system
- Set `USE_PARALLEL = False` for sequential mode (slower but lower resource usage)
- Edit `config/config.py` to change `HEADLESS = False` to see browser automation
- Modify selectors in `scraper.py` if bs.to changes their HTML

## Troubleshooting

### Scraper won't login

- Verify credentials in `.env` are correct
- Check if bs.to requires CAPTCHA (may need manual intervention)
- Ensure Firefox is installed
- Verify geckodriver version matches Firefox version

### Missing series or incorrect watched status

- Ensure you're logged in on the website first
- The selectors might have changed (bs.to updates HTML)
- Approve selectors when prompted on first run
- Check `data/selectors.json` for approved selectors

### Geckodriver issues

- Download geckodriver matching your Firefox version
- Windows: Place `geckodriver.exe` in project root or add to PATH
- Linux/Mac: `chmod +x geckodriver` and add to PATH

### Performance issues

- Reduce `MAX_WORKERS` if system struggles (high RAM/CPU usage)
- Check Firefox process count in Task Manager (should match workers)
- Disable headless mode temporarily to debug: `HEADLESS = False` in `config/config.py`

### "Worker login failed" errors

- Main login might have failed (check credentials)
- Cookie-based auth issue: workers should reuse main session cookies
- Try setting `USE_PARALLEL = False` for sequential mode (slower but more reliable)

## License

This project is for personal use only. Respect the bs.to website's terms of service.
