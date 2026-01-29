import json
import os
from datetime import datetime
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import SERIES_INDEX_FILE, DATA_DIR


def paginate_list(items, formatter, page_size=20):
    """Show items with pagination, Enter = next page, q = skip"""
    if not items:
        return
    total = len(items)
    idx = 0
    while idx < total:
        end = min(idx + page_size, total)
        for item in items[idx:end]:
            print(formatter(item))
        idx = end
        if idx < total:
            choice = input(f"  ({idx}/{total}) Enter = more, q = skip: ").strip().lower()
            if choice == 'q':
                print(f"  ... skipped {total - idx} remaining")
                break


def format_season_ep(season_label, ep_num):
    """
    Format season/episode for display.
    - Regular seasons (Staffel 1, Season 2) → S1E5
    - Special seasons (Specials, OVA, Movies) → [Specials] Ep 3
    """
    import re
    # Try to extract number from season label (Staffel 1, Season 2, etc.)
    match = re.search(r'(staffel|season|s)\s*(\d+)', season_label, re.IGNORECASE)
    if match:
        return f"S{match.group(2)}E{ep_num}"
    else:
        # Special season - show full label
        return f"[{season_label}] Ep {ep_num}"


def group_episodes_by_season(episode_list, new_data):
    """
    Group episodes by series and season, showing count even for partial seasons.
    Returns: list of display strings, already formatted
    """
    from collections import defaultdict
    
    # Group by (title, season)
    grouped = defaultdict(list)
    
    for item in episode_list:
        title, season, ep_num = item[0], item[1], item[2]
        grouped[(title, season)].append(ep_num)
    
    # Convert to dict for new_data lookup
    if isinstance(new_data, list):
        new_data_dict = {s.get('title'): s for s in new_data}
    else:
        new_data_dict = new_data
    
    result = []
    for (title, season), ep_nums in sorted(grouped.items()):
        # Get total episodes in this season from new_data
        series = new_data_dict.get(title, {})
        total_in_season = 0
        
        for s in series.get('seasons', []):
            if s.get('season') == season:
                total_in_season = len(s.get('episodes', []))
                break
        
        # Always show as grouped line with count
        if total_in_season > 0:
            count = len(ep_nums)
            if count == total_in_season:
                # Complete season
                result.append(f"  ✓ {title} [{season}]: {count}/{total_in_season} episodes")
            else:
                # Partial season - still show as one line
                result.append(f"  ✓ {title} [{season}]: {count}/{total_in_season} episodes (partial)")
        else:
            # Fallback if we can't find total - list individual episodes
            for ep_num in sorted(ep_nums):
                result.append(f"  ✓ {title} {format_season_ep(season, ep_num)}")
    
    return result


def print_changes(old_data, new_data):
    """
    Detect and print changes between old and new data with pagination.
    Returns dict with change counts.
    
    Note: We don't track "removed series" because the merge logic preserves all
    existing series. Partial scrapes (single URL, batch) would incorrectly show
    all non-scraped series as "removed".
    """
    changes = {
        "new_series": [],
        "new_episodes": [],
        "newly_watched": [],      # unwatched → watched
        "newly_unwatched": []     # watched → unwatched (needs separate confirmation)
    }
    
    old_titles = set(old_data.keys()) if isinstance(old_data, dict) else {s.get('title') for s in old_data}
    new_titles = set(new_data.keys()) if isinstance(new_data, dict) else {s.get('title') for s in new_data}
    
    # Convert to dicts if needed
    if isinstance(old_data, list):
        old_data = {s.get('title'): s for s in old_data}
    if isinstance(new_data, list):
        new_data = {s.get('title'): s for s in new_data}
    
    # New series (in scraped data but not in existing index)
    for title in new_titles - old_titles:
        changes["new_series"].append(title)
    
    # Episode changes for existing series
    for title in old_titles & new_titles:
        old_series = old_data[title]
        new_series = new_data[title]
        
        old_eps = {}
        for season in old_series.get('seasons', []):
            s_label = season.get('season', '')
            for ep in season.get('episodes', []):
                old_eps[(s_label, ep.get('number'))] = ep.get('watched', False)
        
        # Calculate series progress for context on unwatched changes
        total_eps = sum(1 for eps in old_eps.values())
        watched_eps = sum(1 for w in old_eps.values() if w)
        
        for season in new_series.get('seasons', []):
            s_label = season.get('season', '')
            for ep in season.get('episodes', []):
                ep_num = ep.get('number')
                ep_watched = ep.get('watched', False)
                key = (s_label, ep_num)
                
                if key not in old_eps:
                    changes["new_episodes"].append((title, s_label, ep_num, ep.get('title', '')))
                elif old_eps[key] != ep_watched:
                    if ep_watched:
                        changes["newly_watched"].append((title, s_label, ep_num))
                    else:
                        # Include progress: (title, season, ep, watched_count, total_count)
                        changes["newly_unwatched"].append((title, s_label, ep_num, watched_eps, total_eps))
    
    return changes


def display_changes(changes, include_unwatched=True, new_data=None):
    """Display changes with pagination and smart season grouping"""
    # Count changes (exclude newly_unwatched from main count if not including)
    if include_unwatched:
        total = sum(len(v) for v in changes.values())
    else:
        total = sum(len(v) for k, v in changes.items() if k != 'newly_unwatched')
    
    if total == 0:
        return 0
    
    print("\n" + "="*70)
    print("  CHANGES DETECTED")
    print("="*70)
    
    if changes["new_series"]:
        print(f"\n✨ NEW SERIES ({len(changes['new_series'])})")
        paginate_list(changes["new_series"], lambda title: f"  + {title}")
    
    if changes["new_episodes"]:
        if new_data:
            grouped_lines = group_episodes_by_season([(x[0], x[1], x[2]) for x in changes["new_episodes"]], new_data)
            print(f"\n📺 NEW EPISODES ({len(changes['new_episodes'])})")
            for line in grouped_lines:
                print(line)
        else:
            print(f"\n📺 NEW EPISODES ({len(changes['new_episodes'])}) [ungrouped fallback]")
            for x in changes["new_episodes"]:
                print(f"  + {x[0]} [{x[1]}] Ep {x[2]}")

    if changes["newly_watched"]:
        if new_data:
            # Group by season and summarize fully watched
            from collections import defaultdict
            grouped = defaultdict(list)
            for title, season, ep_num in changes["newly_watched"]:
                grouped[(title, season)].append(ep_num)
            print(f"\n✅ NEWLY WATCHED ({len(changes['newly_watched'])} episodes)")
            for (title, season), ep_nums in grouped.items():
                # Find total episodes in this season
                series = None
                if isinstance(new_data, list):
                    for s in new_data:
                        if s.get('title') == title:
                            series = s
                            break
                else:
                    series = new_data.get(title)
                total_in_season = 0
                if series:
                    for s in series.get('seasons', []):
                        if s.get('season') == season:
                            total_in_season = len(s.get('episodes', []))
                            break
                count = len(ep_nums)
                if total_in_season > 0 and count == total_in_season:
                    print(f"  ✓ {title} [{season}]: {count}/{total_in_season} episodes (100%)")
                else:
                    for ep_num in sorted(ep_nums):
                        print(f"  ✓ {title} [{season}] Ep {ep_num}")
        else:
            print(f"\n✅ NEWLY WATCHED ({len(changes['newly_watched'])}) [ungrouped fallback]")
            for x in changes["newly_watched"]:
                print(f"  ✓ {x[0]} [{x[1]}] Ep {x[2]}")

    # Always show what the site reports as unwatched, even if not allowed
    if changes.get("newly_unwatched"):
        if new_data:
            from collections import defaultdict
            grouped = defaultdict(list)
            for x in changes["newly_unwatched"]:
                grouped[(x[0], x[1])].append(x[2])
            print(f"\n⚠️  SITE REPORTS UNWATCHED ({len(changes['newly_unwatched'])} episodes)")
            for (title, season), ep_nums in grouped.items():
                # Find total episodes in this season
                series = None
                if isinstance(new_data, list):
                    for s in new_data:
                        if s.get('title') == title:
                            series = s
                            break
                else:
                    series = new_data.get(title)
                total_in_season = 0
                if series:
                    for s in series.get('seasons', []):
                        if s.get('season') == season:
                            total_in_season = len(s.get('episodes', []))
                            break
                count = len(ep_nums)
                if total_in_season > 0 and count == total_in_season:
                    print(f"  ⚠ {title} [{season}]: {count}/{total_in_season} episodes (100%)")
                else:
                    for ep_num in sorted(ep_nums):
                        print(f"  ⚠ {title} [{season}] Ep {ep_num}")
        else:
            print(f"\n⚠️  SITE REPORTS UNWATCHED ({len(changes['newly_unwatched'])}) [ungrouped fallback]")
            for x in changes["newly_unwatched"]:
                print(f"  ⚠ {x[0]} [{x[1]}] Ep {x[2]}")
    
    print("\n" + "="*70)
    return total


def confirm_and_save_changes(new_data, description="data"):
    """
    Reusable function to show changes, ask for confirmation, and save.
    Merges new data with existing, preserving watched status by default.
    Watched→unwatched changes require separate confirmation.
    
    Args:
        new_data: List or dict of series to save
        description: What we're saving (for messages)
    
    Returns:
        True if saved, False if cancelled
    """
    # Load existing data
    old_data = {}
    if os.path.exists(SERIES_INDEX_FILE):
        try:
            with open(SERIES_INDEX_FILE, 'r', encoding='utf-8') as f:
                old_list = json.load(f)
            old_data = {s.get('title'): s for s in old_list}
        except Exception as e:
            print(f"  ⚠ Could not load existing data: {str(e)}")
    
    # Convert new_data to dict if needed
    if isinstance(new_data, list):
        new_dict = {s.get('title'): s for s in new_data}
    else:
        new_dict = new_data
    

    # Require manual confirmation for ALL changes (watched and unwatched)
    allow_watched = False
    allow_unwatched = False
    # Confirm watched (unwatched→watched)
    if changes["newly_watched"]:
        print(f"\n✅ {len(changes['newly_watched'])} episode(s) would change from UNWATCHED to WATCHED")
        print("   (manual confirmation required for all watched changes)")
        print("\n" + "-"*70)
        from collections import defaultdict
        grouped = defaultdict(list)
        for x in changes["newly_watched"]:
            grouped[(x[0], x[1])].append(x[2])
        for (title, season), ep_nums in grouped.items():
            print(f"  ✓ {title} [{season}]: {len(ep_nums)} episode(s)")
        print("-"*70)
        watched_response = input("\nAllow these episodes to be marked as WATCHED? (y/n): ").strip().lower()
        if watched_response == 'y':
            allow_watched = True
        else:
            print("  → Watched changes will be ignored (episodes stay unwatched)")

    # Confirm unwatched (watched→unwatched)
    if changes["newly_unwatched"]:
        print(f"\n⚠️  {len(changes['newly_unwatched'])} episode(s) would change from WATCHED to UNWATCHED")
        print("   (manual confirmation required for all unwatched changes)")
        print("\n" + "-"*70)
        from collections import defaultdict
        grouped = defaultdict(list)
        for x in changes["newly_unwatched"]:
            grouped[(x[0], x[1])].append(x[2])
        for (title, season), ep_nums in grouped.items():
            print(f"  ⚠ {title} [{season}]: {len(ep_nums)} episode(s)")
        print("-"*70)
        unwatch_response = input("\nAllow these episodes to be marked as UNWATCHED? (y/n): ").strip().lower()
        if unwatch_response == 'y':
            allow_unwatched = True
        else:
            print("  → Unwatched changes will be ignored (episodes stay watched)")

    # Remove changes not allowed
    if not allow_watched:
        changes["newly_watched"] = []
    if not allow_unwatched:
        changes["newly_unwatched"] = []
    
    # Build merged data (preserve old entries, merge with new)
    merged = dict(old_data)
    for title, new_entry in new_dict.items():
        if title in merged:
            # Merge: only apply watched/unwatched changes if allowed
            old_entry = merged[title]
            old_entry['status'] = 'active'
            old_seasons = {s.get('season'): s for s in old_entry.get('seasons', [])}
            for new_season in new_entry.get('seasons', []):
                season_label = new_season.get('season')
                if season_label in old_seasons:
                    old_eps = {ep.get('number'): ep for ep in old_seasons[season_label].get('episodes', [])}
                    merged_episodes = []
                    for new_ep in new_season.get('episodes', []):
                        ep_num = new_ep.get('number')
                        if ep_num in old_eps:
                            old_watched = old_eps[ep_num].get('watched', False)
                            new_watched = new_ep.get('watched', False)
                            # Only allow watched/unwatched changes if confirmed
                            if allow_watched and (not old_watched and new_watched):
                                new_ep['watched'] = True
                            elif allow_unwatched and (old_watched and not new_watched):
                                new_ep['watched'] = False
                            else:
                                new_ep['watched'] = old_watched
                        merged_episodes.append(new_ep)
                    old_seasons[season_label]['episodes'] = merged_episodes
                else:
                    old_seasons[season_label] = new_season
            old_entry['seasons'] = list(old_seasons.values())
            old_entry['watched_episodes'] = sum(
                sum(1 for ep in s.get('episodes', []) if ep.get('watched'))
                for s in old_entry['seasons']
            )
            old_entry['total_episodes'] = sum(s.get('total_episodes', 0) for s in old_entry['seasons'])
            old_entry['url'] = new_entry.get('url', old_entry.get('url'))
            old_entry['last_updated'] = datetime.now().isoformat()
        else:
            # New series
            new_entry['added_date'] = datetime.now().isoformat()
            new_entry['status'] = 'active'
            merged[title] = new_entry
    
    # Show final changes summary (excluding unwatched if not allowed)
    main_changes = sum(len(v) for k, v in changes.items() if k != 'newly_unwatched')
    if allow_unwatched:
        main_changes += len(changes['newly_unwatched'])
    
    if main_changes == 0:
        print(f"\n✓ {description} already up to date.")
        return True
    
    # Display changes (without unwatched section, already handled above)
    display_changes(changes, include_unwatched=False, new_data=new_dict)
    
    # Ask for confirmation
    response = input(f"\nSave these changes? (y/n): ").strip().lower()
    if response != 'y':
        print("✗ Changes discarded. Nothing saved.")
        return False
    
    # Save merged data
    try:
        series_list = list(merged.values())
        with open(SERIES_INDEX_FILE, 'w', encoding='utf-8') as f:
            json.dump(series_list, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved {len(series_list)} series to index")
        return True
    except Exception as e:
        print(f"✗ Failed to save: {str(e)}")
        return False


class IndexManager:
    def __init__(self):
        self.series_index = {}
        self.ensure_data_dir()
        self.load_index()
        
    def ensure_data_dir(self):
        """Create data directory if it doesn't exist"""
        os.makedirs(DATA_DIR, exist_ok=True)
        
    def load_index(self):
        """Load existing series index"""
        if os.path.exists(SERIES_INDEX_FILE):
            try:
                with open(SERIES_INDEX_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Convert list format to dict format if needed
                if isinstance(data, list):
                    # Old format: list of series with 'title' key
                    self.series_index = {item.get("title"): item for item in data}
                else:
                    # Already dict format
                    self.series_index = data
                
                print(f"✓ Loaded {len(self.series_index)} series from index")
            except Exception as e:
                print(f"⚠ Error loading index: {str(e)}")
                self.series_index = {}
                
    def create_index_from_scraped_data(self, scraped_series):
        """Create index from scraped data - updates episode data for existing series"""
        print(f"→ Creating index from {len(scraped_series)} series...")
        
        new_series = 0
        updated_series = 0
        
        for series in scraped_series:
            title = series.get("title", "Unknown")
            
            if title not in self.series_index:
                # New series: use all data from scraper
                new_series += 1
                series["added_date"] = datetime.now().isoformat()
                self.series_index[title] = series
            else:
                # Existing series: merge new episode data, preserve added_date
                updated_series += 1
                old_added_date = self.series_index[title].get("added_date")
                series["last_updated"] = datetime.now().isoformat()
                if old_added_date:
                    series["added_date"] = old_added_date
                self.series_index[title] = series
                
        print(f"✓ Index created: {new_series} new, {updated_series} updated")
        return self.series_index
        
    def get_statistics(self):
        """Compute stats based on episode progress (no separate watched list)"""
        series_with_progress = self.get_series_with_progress()
        total = len(series_with_progress)
        watched = sum(1 for s in series_with_progress if not s['is_incomplete'])
        unwatched = total - watched
        empty_count = len([s for s in self.series_index.values() if s.get('empty', False)])

        return {
            "total_series": total,
            "watched": watched,
            "unwatched": unwatched,
            "watched_percentage": round((watched / total * 100) if total > 0 else 0, 2),
            "empty_series": empty_count
        }
        
    def save_index(self):
        """Save index to file as list (matching scraper format)"""
        try:
            # Convert dict back to list for storage
            series_list = list(self.series_index.values())
            with open(SERIES_INDEX_FILE, 'w', encoding='utf-8') as f:
                json.dump(series_list, f, indent=2, ensure_ascii=False)
            print(f"✓ Saved series index ({len(self.series_index)} entries)")
        except Exception as e:
            print(f"✗ Failed to save index: {str(e)}")
            

            
    def save_all(self):
        """Save series index"""
        self.save_index()
        
    def get_full_report(self):
        """Generate a full report of all series including ongoing/started"""
        series_progress = self.get_series_with_progress()
        watched_titles = [s['title'] for s in series_progress if not s['is_incomplete']]
        ongoing_series = [s for s in series_progress if s['is_incomplete'] and s['watched_episodes'] > 0]
        not_started = [s for s in series_progress if s['is_incomplete'] and s['watched_episodes'] == 0]
        
        # Sort ongoing by completion % (descending)
        ongoing_titles = sorted([s['title'] for s in ongoing_series], 
                               key=lambda x: next((s['completion'] for s in ongoing_series if s['title'] == x), 0), 
                               reverse=True)
        not_started_titles = sorted([s['title'] for s in not_started])

        report = {
            "metadata": {
                "generated": datetime.now().isoformat(),
                "statistics": self.get_statistics()
            },
            "watched": sorted(watched_titles),
            "ongoing": ongoing_titles,
            "not_started": not_started_titles,
            "all_series": self.series_index
        }
        return report
        
    def get_series_with_progress(self, sort_by='completion', reverse=False):
        """
        Get series with episode progress information
        
        Args:
            sort_by: 'completion', 'name', 'date', 'total_episodes'
            reverse: Sort in descending order
            
        Returns:
            List of series with: title, total_episodes, watched_episodes, completion_percent, is_incomplete
        """
        series_list = []
        
        for title, data in self.series_index.items():
            if data.get('empty', False):
                continue
            
            # Episodes may be stored either at top-level or nested under seasons
            episodes = []
            if isinstance(data.get('episodes'), list):
                episodes = data.get('episodes', [])
            elif isinstance(data.get('seasons'), list):
                for season in data.get('seasons', []):
                    if isinstance(season, dict) and isinstance(season.get('episodes'), list):
                        episodes.extend(season.get('episodes', []))
            
            total_episodes = len(episodes)
            if total_episodes == 0:
                continue
            
            watched_episodes = sum(1 for ep in episodes if ep.get('watched', False))
            completion = round((watched_episodes / total_episodes * 100), 1) if total_episodes > 0 else 0
            is_incomplete = watched_episodes < total_episodes
            
            series_list.append({
                'title': title,
                'total_episodes': total_episodes,
                'watched_episodes': watched_episodes,
                'completion': completion,
                'is_incomplete': is_incomplete,
                'added_date': data.get('added_date', 'N/A'),
                'url': data.get('url', '')
            })
        
        # Sort by specified criteria
        if sort_by == 'completion':
            series_list.sort(key=lambda x: (x['is_incomplete'], x['completion']), reverse=not reverse)
        elif sort_by == 'name':
            series_list.sort(key=lambda x: x['title'], reverse=reverse)
        elif sort_by == 'date':
            series_list.sort(key=lambda x: x['added_date'], reverse=not reverse)
        elif sort_by == 'total_episodes':
            series_list.sort(key=lambda x: x['total_episodes'], reverse=not reverse)
        elif sort_by == 'total_episodes_desc':
            series_list.sort(key=lambda x: x['total_episodes'], reverse=True)
            
        return series_list
