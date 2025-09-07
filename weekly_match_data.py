# Weekly NCAA Match Results Collection
# Collects match results and saves them weekly to Data/Matches folder

import time
import json
import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# --- helpers ---

def _extract_boxscore_id(url_path: str) -> str:
    if not url_path:
        return ""
    parts = url_path.strip("/").split("/")
    return parts[-1] if parts and parts[-1].isdigit() else ""

def _safe_int(x):
    try:
        return int(float(str(x)))
    except Exception:
        return 0

# ====================
# Weekly Period Functions
# ====================
def get_week_periods(start_date_str, end_date_str):
    """Generate weekly periods and return the end date of each week"""
    start = datetime.strptime(start_date_str, '%Y-%m-%d')
    end = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    weeks = []
    current = start
    
    while current <= end:
        # Find the end of the current week (Sunday)
        days_until_sunday = 6 - current.weekday()  # Monday = 0, Sunday = 6
        week_end = current + timedelta(days=days_until_sunday)
        
        # Don't go beyond the actual end date
        if week_end > end:
            week_end = end
            
        weeks.append({
            'start': current.strftime('%Y-%m-%d'),
            'end': week_end.strftime('%Y-%m-%d'),
            'filename': week_end.strftime('%Y%m%d')
        })
        
        # Move to the start of next week
        current = week_end + timedelta(days=1)
        
    return weeks

# --- scores from scoreboard.json (EXPOSES boxscore_id) ---

def get_scores(gender, division, month, day):
    url = f"https://data.ncaa.com/casablanca/scoreboard/soccer-{gender}/{division}/2025/{month}/{day}/scoreboard.json"

    r = requests.get(
        url,
        timeout=20,
        headers={"User-Agent": "Mozilla/5.0"}
    )
    if r.status_code != 200:
        return []

    data = r.json()
    out = []
    for wrap in data.get("games", []):
        game = wrap.get("game", {})
        home = game.get("home", {})
        away = game.get("away", {})
        boxscore_id = _extract_boxscore_id(game.get("url", ""))

        out.append({
            "gender": gender,
            "division": division,
            "game_id": game.get("gameID", ""),
            "boxscore_id": boxscore_id,          # <-- use this to fetch boxscore.json
            "date": game.get("startDate", ""),
            "status": game.get("gameState", ""),
            "start_time": game.get("startTime", ""),
            "home_team_full": home.get("names", {}).get("full", ""),
            "home_team_short": home.get("names", {}).get("short", ""),
            "home_team_score": home.get("score", ""),
            "home_record": home.get("description", ""),
            "home_conference": (home.get("conferences", [{}])[0] or {}).get("conferenceName", ""),
            "away_team_full": away.get("names", {}).get("full", ""),
            "away_team_short": away.get("names", {}).get("short", ""),
            "away_team_score": away.get("score", ""),
            "away_record": away.get("description", ""),
            "away_conference": (away.get("conferences", [{}])[0] or {}).get("conferenceName", ""),
        })
    return out

# --- shots/SOT using boxscore_id AND extract scores if missing ---

def get_match_shots_sot_by_boxscore(boxscore_ids, max_retries=2, backoff=1.0):
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
        "Accept": "application/json,text/*,*/*;q=0.9",
        "Connection": "keep-alive",
    }

    rows = []
    for bid in boxscore_ids:
        bid = str(bid).strip()
        if not bid.isdigit():
            continue

        url = f"https://data.ncaa.com/casablanca/game/{bid}/boxscore.json"
        data = None

        # Try requests with Referer header to look more like a browser hit on that game page
        req_headers = headers | {"Referer": f"https://www.ncaa.com/game/{bid}"}

        for attempt in range(max_retries + 1):
            try:
                resp = requests.get(url, headers=req_headers, timeout=20)
                if resp.status_code == 200:
                    data = resp.json()
                    break
                elif resp.status_code in (403, 429, 503):
                    time.sleep(backoff * (attempt + 1))
                    continue
                else:
                    break
            except requests.RequestException:
                time.sleep(backoff * (attempt + 1))
                continue

        # last resort: curl
        if data is None:
            try:
                result = os.popen(
                    f'curl -s -H "User-Agent: {headers["User-Agent"]}" '
                    f'-H "Referer: https://www.ncaa.com/game/{bid}" {url}'
                ).read()
                data = json.loads(result)
            except Exception:
                data = None

        # If still no valid data, skip cleanly
        if not data or "meta" not in data or "teams" not in data:
            continue

        # map teamId -> (is_home, shortName)
        id_to_side = {}
        for t in data["meta"].get("teams", []):
            tid = str(t.get("id", ""))
            is_home = (t.get("homeTeam") == "true")
            short_name = t.get("shortName", "")
            id_to_side[tid] = (is_home, short_name)

        home_shots = home_sot = away_shots = away_sot = 0
        home_short = away_short = ""
        # NEW: Initialize scores from boxscore data
        home_goals_boxscore = away_goals_boxscore = None

        for team_block in data.get("teams", []):
            tid = str(team_block.get("teamId", ""))
            is_home, short_name = id_to_side.get(tid, (None, ""))

            t_shots = t_sot = 0
            for p in team_block.get("playerStats", []):
                t_shots += _safe_int(p.get("shots"))
                t_sot   += _safe_int(p.get("shotsOnGoal"))

            # NEW: Extract goals from playerTotals if available
            player_totals = team_block.get("playerTotals", {})
            team_goals = _safe_int(player_totals.get("goals", 0))

            if is_home is True:
                home_shots += t_shots
                home_sot   += t_sot
                home_short   = short_name
                home_goals_boxscore = team_goals
            elif is_home is False:
                away_shots += t_shots
                away_sot   += t_sot
                away_short   = short_name
                away_goals_boxscore = team_goals

        row_data = {
            "boxscore_id": bid,                 # <-- merge key
            # We COULD return team_short here, but we'll drop them at merge time to avoid duplicates
            "home_team_short": home_short,
            "away_team_short": away_short,
            "home_shots": home_shots,
            "home_sot": home_sot,
            "away_shots": away_shots,
            "away_sot": away_sot,
        }
        
        # NEW: Add boxscore goals if available
        if home_goals_boxscore is not None:
            row_data["home_goals_boxscore"] = home_goals_boxscore
        if away_goals_boxscore is not None:
            row_data["away_goals_boxscore"] = away_goals_boxscore
            
        rows.append(row_data)

    return rows

# --- Function to collect match data for a date range ---

def collect_match_data_for_period(start_date, end_date):
    """Collect match results for a specific date range."""
    date_range = pd.date_range(start=start_date, end=end_date)
    divisions  = ['d1', 'd2', 'd3']
    genders    = ['men', 'women']

    men_dfs, women_dfs = [], []

    for gender in genders:
        for division in divisions:
            for date in date_range:
                month = date.strftime("%m")
                day   = date.strftime("%d")

                scores = get_scores(gender, division, month, day)
                if not scores:
                    continue

                scores_df = pd.DataFrame(scores)

                # Use the correct IDs
                box_ids = scores_df.get('boxscore_id')
                if box_ids is None:
                    merged = scores_df
                else:
                    box_ids = box_ids.dropna().astype(str).unique().tolist()
                    shots_rows = get_match_shots_sot_by_boxscore(box_ids)
                    if shots_rows:
                        shots_df = pd.DataFrame(shots_rows)

                        # --- DROP duplicate team short-name columns before merge ---
                        for c in ["home_team_short", "away_team_short"]:
                            if c in shots_df.columns:
                                shots_df = shots_df.drop(columns=[c])

                        merged = scores_df.merge(shots_df, on='boxscore_id', how='left')
                        
                        # NEW: Use boxscore goals if original scores are empty/null (but not if they're legitimate 0s)
                        if 'home_goals_boxscore' in merged.columns:
                            # Fill empty home_team_score with boxscore goals
                            # Only replace if the score is truly missing (null or empty string), not if it's a legitimate 0
                            mask = (merged['home_team_score'].isna()) | (merged['home_team_score'] == '')
                            merged.loc[mask, 'home_team_score'] = merged.loc[mask, 'home_goals_boxscore']
                            
                        if 'away_goals_boxscore' in merged.columns:
                            # Fill empty away_team_score with boxscore goals
                            # Only replace if the score is truly missing (null or empty string), not if it's a legitimate 0
                            mask = (merged['away_team_score'].isna()) | (merged['away_team_score'] == '')
                            merged.loc[mask, 'away_team_score'] = merged.loc[mask, 'away_goals_boxscore']
                    else:
                        merged = scores_df

                if gender == "men":
                    men_dfs.append(merged)
                else:
                    women_dfs.append(merged)

    men_df   = pd.concat(men_dfs, ignore_index=True) if men_dfs else pd.DataFrame()
    women_df = pd.concat(women_dfs, ignore_index=True) if women_dfs else pd.DataFrame()

    # Optional: ensure numeric types for shots columns
    for df in [men_df, women_df]:
        for c in ["home_shots", "home_sot", "away_shots", "away_sot"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

    return men_df, women_df

# ====================
# Main execution
# ====================
def main():
    # Configuration
    start_date = '2025-08-14'
    end_date = '2025-09-03'
    
    # Create Data directories if they don't exist
    os.makedirs('Data/Matches', exist_ok=True)
    
    # Generate weekly periods
    weekly_periods = get_week_periods(start_date, end_date)
    
    print(f"Processing {len(weekly_periods)} weekly periods:")
    for week in weekly_periods:
        print(f"  Week ending {week['end']} -> file suffix {week['filename']}")
    
    # Process each week
    for week in weekly_periods:
        week_start = week['start']
        week_end = week['end']
        filename_suffix = week['filename']
        
        print(f"\nProcessing week: {week_start} to {week_end}")
        
        # Collect match data for this week
        mens_df, womens_df = collect_match_data_for_period(week_start, week_end)
        
        # Combine all matches for this week
        all_matches = []
        if not mens_df.empty:
            all_matches.append(mens_df)
        if not womens_df.empty:
            all_matches.append(womens_df)
        
        if all_matches:
            combined_df = pd.concat(all_matches, ignore_index=True)
            
            # Clean up temporary boxscore columns before saving
            combined_df = combined_df.drop(columns=['home_goals_boxscore', 'away_goals_boxscore'], errors='ignore')
            
            # Save the data
            matches_filename = f"Data/Matches/matches_{filename_suffix}.csv"
            combined_df.to_csv(matches_filename, index=False)
            
            print(f"Saved: {matches_filename}")
            print(f"  - Total matches: {len(combined_df)}")
            print(f"  - Men's matches: {len(mens_df) if not mens_df.empty else 0}")
            print(f"  - Women's matches: {len(womens_df) if not womens_df.empty else 0}")
        else:
            print(f"No matches found for week ending {week_end}")

    print("\nWeekly match data collection completed!")


if __name__ == "__main__":
    main()
