# Weekly NCAA Match Results Collection
# Collects match results and saves them weekly to Data/Matches folder

import time
import json
import os
import subprocess
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests
import pandas as pd
from datetime import datetime, timedelta

# --- helpers ---

# Verbose progress output controls
VERBOSE = True
PROGRESS_EVERY = 10  # print progress every N boxscores

# Performance toggles
ENRICH_WITH_BOXSCORES = True  # Set False to skip fetching boxscore.json (faster)
MAX_WORKERS = 8               # Parallel requests for boxscore fetches
REQUEST_TIMEOUT = 12          # Seconds per request

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

def _clean_score(val):
    s = "" if val is None else str(val).strip()
    if s in ("", "-", "—", "NA", "null", "None"):
        return None
    try:
        return int(float(s))
    except Exception:
        return None

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
        # Clean scores: some feeds return '-' or '' when in-progress or missing
        h_score = _clean_score(home.get("score", ""))
        a_score = _clean_score(away.get("score", ""))

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
            "home_team_score": h_score,
            "home_record": home.get("description", ""),
            "home_conference": (home.get("conferences", [{}])[0] or {}).get("conferenceName", ""),
            "away_team_full": away.get("names", {}).get("full", ""),
            "away_team_short": away.get("names", {}).get("short", ""),
            "away_team_score": a_score,
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

    def _sdataprod_url(contest_id: str):
        meta = {"persistedQuery": {"version": 1, "sha256Hash": "c9070c4e5a76468a4025896df89f8a7b22be8275c54a22ff79619cbb27d63d7d"}}
        variables = {"contestId": str(contest_id), "staticTestEnv": None}
        return (
            "https://sdataprod.ncaa.com/?meta=NCAA_GetGamecenterBoxscoreSoccerById_web"
            + "&extensions=" + urllib.parse.quote(json.dumps(meta), safe="")
            + "&variables=" + urllib.parse.quote(json.dumps(variables), safe="")
        )

    def _extract_from_sdataprod(data):
        """Extract shots, SOG, and goals from sdataprod GraphQL payload.
        Primary shape: data.boxscore.{teams, teamBoxscore}.
        Falls back to legacy generic scan if not present.
        """
        def pick(d, keys, default=None):
            for k in keys:
                if isinstance(d, dict) and k in d and d[k] not in (None, ""):
                    return d[k]
            return default

        # Preferred parsing path
        box = pick(data or {}, ['data'], {})
        box = pick(box or {}, ['boxscore'], None)
        if isinstance(box, dict) and 'teams' in box and 'teamBoxscore' in box:
            # Build teamId -> (is_home, shortName)
            id_to_side = {}
            for t in box.get('teams', []) or []:
                tid = str(pick(t, ['teamId','id','tid'], '') or '')
                is_home = bool(t.get('isHome'))
                short = pick(t, ['nameShort','shortName','shortDisplayName','teamName'], '') or ''
                id_to_side[tid] = (is_home, short)

            home_shots = home_sot = away_shots = away_sot = 0
            home_short = away_short = ''
            home_goals = away_goals = None

            for tb in box.get('teamBoxscore', []) or []:
                tid = str(pick(tb, ['teamId','id','tid'], '') or '')
                side = id_to_side.get(tid)
                if side is None:
                    continue
                is_home, short = side

                # Prefer teamStats for totals, fallback to summing players
                tstats = tb.get('teamStats') or {}
                shots = pick(tstats, ['shots','totalShots','shotAttempts'], None)
                sog   = pick(tstats, ['shotsOnGoal','shotsOnTarget','sog'], None)
                goals = pick(tstats, ['goals','score','teamScore','points'], None)

                if shots is None or sog is None:
                    # sum from players
                    pshots = psog = 0
                    for p in tb.get('playerStats', []) or []:
                        pshots += _safe_int(p.get('shots'))
                        psog   += _safe_int(p.get('shotsOnGoal'))
                    shots = pshots if shots is None else shots
                    sog   = psog if sog is None else sog

                if is_home:
                    home_shots += _safe_int(shots)
                    home_sot   += _safe_int(sog)
                    home_short   = short
                    home_goals   = _safe_int(goals) if goals is not None else home_goals
                else:
                    away_shots += _safe_int(shots)
                    away_sot   += _safe_int(sog)
                    away_short   = short
                    away_goals   = _safe_int(goals) if goals is not None else away_goals

            return {
                'home_short': home_short,
                'away_short': away_short,
                'home_shots': home_shots,
                'home_sot': home_sot,
                'away_shots': away_shots,
                'away_sot': away_sot,
                'home_goals_boxscore': home_goals,
                'away_goals_boxscore': away_goals,
            }

        # Fallback: legacy generic scan for 'home'/'away' shaped nodes
        def iter_nodes(obj):
            if isinstance(obj, dict):
                yield obj
                for v in obj.values():
                    yield from iter_nodes(v)
            elif isinstance(obj, list):
                for v in obj:
                    yield from iter_nodes(v)

        home = away = None
        for node in iter_nodes(data):
            if isinstance(node, dict):
                if 'home' in node and 'away' in node and isinstance(node['home'], dict) and isinstance(node['away'], dict):
                    home, away = node['home'], node['away']
                    break
                if 'homeTeam' in node and 'awayTeam' in node and isinstance(node['homeTeam'], dict) and isinstance(node['awayTeam'], dict):
                    home, away = node['homeTeam'], node['awayTeam']
                    break
        if not home or not away:
            return None

        def team_short(d):
            team = d.get('team') if isinstance(d.get('team'), dict) else d
            return pick(team or {}, ['shortName','shortDisplayName','seoName','nickName'], '') or ''

        def totals(d):
            src = d.get('totals') if isinstance(d.get('totals'), dict) else d
            shots = pick(src, ['shots','totalShots','shotAttempts'], 0)
            sog   = pick(src, ['shotsOnGoal','shotsOnTarget','sog'], 0)
            goals = pick(src, ['goals','score','teamScore','points'], None)
            return shots, sog, goals

        h_shots, h_sog, h_goals = totals(home)
        a_shots, a_sog, a_goals = totals(away)
        return {
            'home_short': team_short(home),
            'away_short': team_short(away),
            'home_shots': _safe_int(h_shots),
            'home_sot': _safe_int(h_sog),
            'away_shots': _safe_int(a_shots),
            'away_sot': _safe_int(a_sog),
            'home_goals_boxscore': None if h_goals is None else _safe_int(h_goals),
            'away_goals_boxscore': None if a_goals is None else _safe_int(a_goals),
        }

    def fetch_one(bid: str):
        bid = str(bid).strip()
        if not bid.isdigit():
            return None

        # 1) Try SDATA GraphQL endpoint first
        data = None
        try:
            sdata_url = _sdataprod_url(bid)
            resp = requests.get(sdata_url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": headers["User-Agent"]})
            if resp.status_code == 200:
                sdata = resp.json()
                parsed = _extract_from_sdataprod(sdata)
                if parsed:
                    parsed['boxscore_id'] = bid
                    return parsed
        except Exception:
            pass

        # 2) Fallback to casablanca boxscore.json
        url = f"https://data.ncaa.com/casablanca/game/{bid}/boxscore.json"
        data = None
        req_headers = headers | {"Referer": f"https://www.ncaa.com/game/{bid}"}

        for attempt in range(max_retries + 1):
            try:
                resp = requests.get(url, headers=req_headers, timeout=REQUEST_TIMEOUT)
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

        # last resort: curl with timeout to avoid hanging
        if data is None:
            try:
                cmd = [
                    'curl','-s',
                    '-H', f'User-Agent: {headers["User-Agent"]}',
                    '-H', f'Referer: https://www.ncaa.com/game/{bid}',
                    url
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=REQUEST_TIMEOUT)
                if result.returncode == 0 and result.stdout:
                    data = json.loads(result.stdout)
            except Exception:
                data = None

    # If still no valid data, skip cleanly
        if not data or "meta" not in data or "teams" not in data:
            return None

        # map teamId -> (is_home, shortName)
        id_to_side = {}
        for t in data["meta"].get("teams", []):
            tid = str(t.get("id", ""))
            is_home = (t.get("homeTeam") == "true")
            short_name = t.get("shortName", "")
            id_to_side[tid] = (is_home, short_name)

        home_shots = home_sot = away_shots = away_sot = 0
        home_short = away_short = ""
        # Initialize scores from boxscore data
        home_goals_boxscore = away_goals_boxscore = None

        for team_block in data.get("teams", []):
            tid = str(team_block.get("teamId", ""))
            is_home, short_name = id_to_side.get(tid, (None, ""))

            t_shots = t_sot = 0
            for p in team_block.get("playerStats", []):
                t_shots += _safe_int(p.get("shots"))
                t_sot   += _safe_int(p.get("shotsOnGoal"))

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
            "boxscore_id": bid,
            "home_team_short": home_short,
            "away_team_short": away_short,
            "home_shots": home_shots,
            "home_sot": home_sot,
            "away_shots": away_shots,
            "away_sot": away_sot,
        }
        if home_goals_boxscore is not None:
            row_data["home_goals_boxscore"] = home_goals_boxscore
        if away_goals_boxscore is not None:
            row_data["away_goals_boxscore"] = away_goals_boxscore
        return row_data

    rows = []
    total = len(boxscore_ids)
    if total == 0:
        return rows

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_one, bid): bid for bid in boxscore_ids}
        done = 0
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                rows.append(res)
            done += 1
            if VERBOSE and (done % PROGRESS_EVERY == 0 or done == total):
                print(f"  [boxscore] processed {done}/{total}", flush=True)

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
                    if VERBOSE:
                        print(f"[{gender}/{division} {month}/{day}] no games", flush=True)
                    continue

                scores_df = pd.DataFrame(scores)
                if VERBOSE:
                    print(f"[{gender}/{division} {month}/{day}] games: {len(scores_df)}", flush=True)

                # Use the correct IDs
                box_ids = scores_df.get('boxscore_id')
                if box_ids is None:
                    merged = scores_df
                else:
                    box_ids = box_ids.dropna().astype(str).unique().tolist()
                    if VERBOSE:
                        print(f"  fetching boxscores: {len(box_ids)} ids", flush=True)
                    shots_rows = get_match_shots_sot_by_boxscore(box_ids) if ENRICH_WITH_BOXSCORES and box_ids else []
                    if shots_rows:
                        shots_df = pd.DataFrame(shots_rows)

                        # --- DROP duplicate team short-name columns before merge ---
                        for c in ["home_team_short", "away_team_short"]:
                            if c in shots_df.columns:
                                shots_df = shots_df.drop(columns=[c])

                        merged = scores_df.merge(shots_df, on='boxscore_id', how='left')
                        
                        # NEW: Use boxscore goals if original scores missing/None
                        if 'home_goals_boxscore' in merged.columns:
                            mask = merged['home_team_score'].isna()
                            merged.loc[mask, 'home_team_score'] = merged.loc[mask, 'home_goals_boxscore']

                        if 'away_goals_boxscore' in merged.columns:
                            mask = merged['away_team_score'].isna()
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
    start_date = '2025-09-01'
    end_date = '2025-09-07'

    # Create data/Matches directory relative to repo root
    root_dir = Path(__file__).resolve().parent.parent
    matches_dir = root_dir / 'data' / 'Matches'
    matches_dir.mkdir(parents=True, exist_ok=True)
    
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
            
            # Coerce scores to numeric and fill remaining NaNs sensibly
            for col in ['home_team_score','away_team_score']:
                if col in combined_df.columns:
                    combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')

            # Clean up temporary boxscore columns before saving
            combined_df = combined_df.drop(columns=['home_goals_boxscore', 'away_goals_boxscore'], errors='ignore')
            
            # Save the data
            matches_filename = matches_dir / f"matches_{filename_suffix}.csv"
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
