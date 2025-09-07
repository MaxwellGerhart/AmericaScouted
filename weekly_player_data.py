# Weekly NCAA Player Data Collection
# Collects player statistics and saves them weekly to Data/Players folder

import warnings
warnings.filterwarnings('ignore')

import time
import json
import os
import re
from collections import defaultdict, Counter
from datetime import datetime, timedelta

import requests
import pandas as pd
from unidecode import unidecode
from rapidfuzz import process, fuzz


# ====================
# Config
# ====================
START_DATE = '2025-08-14'
END_DATE = '2025-09-03'
GENDERS = ['men', 'women']
DIVISIONS = ['d1', 'd2', 'd3']

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
REQ_HEADERS = {"User-Agent": UA, "Accept": "application/json,text/*,*/*;q=0.9", "Connection": "keep-alive"}

SESSION = requests.Session()
SESSION.headers.update(REQ_HEADERS)


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


# ====================
# Name utilities
# ====================
def preprocess_name(name):
    """Lowercase, strip, de-accent. Robust to NaN/non-strings."""
    if pd.isna(name):
        return ""
    return unidecode(str(name)).strip().lower()

def clean_name(name):
    """Title-cased, de-accented, and swaps 'Last, First' → 'First Last'."""
    if not name:
        return ""
    s = str(name)
    if ', ' in s:
        s = ' '.join(s.split(', ')[::-1])
    return unidecode(s).strip().title()

def create_name_mapping(*name_lists, similarity_threshold=90):
    """Map preprocessed names to canonical tokens using fuzzy matching."""
    all_tokens = set()
    for names in name_lists:
        all_tokens.update(preprocess_name(n) for n in names if n is not None)
    standardized = {}
    for token in list(all_tokens):
        if not standardized:
            standardized[token] = token
            continue
        match = process.extractOne(token, list(standardized.keys()), scorer=fuzz.WRatio)
        if match and match[1] > similarity_threshold:
            standardized[token] = standardized[match[0]]
        else:
            standardized[token] = token
    return standardized

def apply_name_mapping(names, name_mapping):
    return [name_mapping.get(preprocess_name(n), preprocess_name(n)) for n in names]


# ====================
# Position helpers
# ====================
def dominant_position_single(pos):
    """Map a single raw position string to a coarse label."""
    if pd.isna(pos) or not isinstance(pos, str) or len(pos) == 0:
        return "Unknown"
    position_keywords = {
        'Midfielder': ['M', 'MIDFIELDER'],
        'Defender': ['D', 'DEFENDER'],
        'Forward': ['F', 'FORWARD'],
        'Goalkeeper': ['G', 'GK', 'GOALKEEPER']
    }
    up = pos.upper()
    counts = {lab: sum(up.count(k) for k in keys) for lab, keys in position_keywords.items()}
    if all(v == 0 for v in counts.values()):
        return "Unknown"
    return max(counts, key=counts.get)

POS_TIEBREAK = {'Goalkeeper': 0, 'Defender': 1, 'Midfielder': 2, 'Forward': 3, 'Unknown': 9}

def compute_dominant_position_over_games(positions_series: pd.Series) -> str:
    """Choose the most frequent coarse label across all games; break ties by POS_TIEBREAK."""
    if positions_series is None or positions_series.empty:
        return "Unknown"
    labels = positions_series.map(dominant_position_single)
    if labels.empty:
        return "Unknown"
    counts = labels.value_counts(dropna=False)
    max_count = counts.max()
    candidates = [lab for lab, cnt in counts.items() if cnt == max_count]
    return sorted(candidates, key=lambda lab: POS_TIEBREAK.get(lab, 9))[0]


# ====================
# Scoreboard (game ids + teams) & votes for team→division
# ====================
def get_day_games(gender: str, day_mmdd: str, division: str):
    """Return list of {'gid': str, 'teams': [{'id': str, 'shortName': str}, ...]} for a day/division."""
    month, day = day_mmdd.split("/")
    url = f"https://data.ncaa.com/casablanca/scoreboard/soccer-{gender}/{division}/2025/{month}/{day}/scoreboard.json"
    out = []
    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        for g in data.get("games", []) or []:
            game_url = (g.get("game", {}) or {}).get("url", "")
            gid = game_url.strip("/").split("/")[-1] if game_url else ""
            teams = []
            for t in g.get("teams", []) or []:
                tid = str(t.get("id") or t.get("teamId") or "").strip()
                short = (t.get("shortName") or t.get("seoName") or t.get("nickName") or "").strip()
                if tid:
                    teams.append({"id": tid, "shortName": short})
            if gid:
                out.append({"gid": gid, "teams": teams})
    except Exception as e:
        print(f"Error fetching day games: {e} ({gender} {division} {day_mmdd})")
    return out


# ====================
# Boxscore → per-player rows (incl. GK stats); adds Game ID, Team ID
# ====================
def _first_present(d, keys, default=0):
    for k in keys:
        if k in d and d[k] not in (None, "", "NA"):
            try:
                return int(float(str(d[k])))
            except Exception:
                pass
    return default

def clean_data_from_boxscore_payload(data, gid):
    # meta teams (for team names)
    home_id = str(data['meta']['teams'][0]['id'])
    away_id = str(data['meta']['teams'][1]['id'])
    home_nm = data['meta']['teams'][0].get('shortName', '')
    away_nm = data['meta']['teams'][1].get('shortName', '')

    def _team_name_for(team_id_str):
        return home_nm if team_id_str == home_id else (away_nm if team_id_str == away_id else "")

    out = []
    for team in data.get('teams', []) or []:
        tid = str(team.get('teamId', '')).strip()
        team_name = _team_name_for(tid)
        if not team_name:
            continue

        # GK lookup
        gk_by_name = {}
        for g in team.get('goalieStats', []) or []:
            raw_name = g.get('name') or f"{g.get('firstName','')} {g.get('lastName','')}"
            name = clean_name(raw_name)
            gk_by_name[name] = {
                'Saves': _first_present(g, ['saves'], 0),
                'Goals Against': _first_present(g, ['goalsAllowed','goalsAgainst','ga'], 0),
                'Minutes GK': _first_present(g, ['minutesAtGoalie','minutes'], 0),
                'Jersey': str(g.get('jerseyNum','')).strip(),
            }

        # Fallback: goalieTotals + exactly one GK in playerStats
        if not gk_by_name:
            gt = team.get('goalieTotals') or {}
            tot_saves = _first_present(gt, ['saves'], 0)
            tot_ga = _first_present(gt, ['goalsAllowed','goalsAgainst','ga'], 0)
            gk_players = [p for p in (team.get('playerStats', []) or [])
                          if dominant_position_single((p.get('position') or '').strip()) == 'Goalkeeper']
            if len(gk_players) == 1:
                p0 = gk_players[0]
                name0 = clean_name(f"{p0.get('firstName','')} {p0.get('lastName','')}".strip())
                gk_by_name[name0] = {
                    'Saves': tot_saves,
                    'Goals Against': tot_ga,
                    'Minutes GK': _first_present(p0, ['minutesPlayed','minutes'], 0),
                    'Jersey': str(p0.get('jerseyNum','')).strip(),
                }

        # Emit player rows
        for p in team.get('playerStats', []) or []:
            full_name = clean_name(f"{p.get('firstName','')} {p.get('lastName','')}".strip())
            pos = (p.get('position') or '').strip()

            row = {
                'Game ID': gid,
                'Team ID': tid,
                'Name': full_name,
                'Team': team_name,
                'Position': pos,  # keep raw; do NOT compute dominant here
                'Matches Played': 1 if _first_present(p, ['minutesPlayed','minutes'], 0) > 0 else 0,
                'Minutes Played': _first_present(p, ['minutesPlayed','minutes'], 0),
                'Goals': _first_present(p, ['goals'], 0),
                'Assists': _first_present(p, ['assists'], 0),
                'Shots': _first_present(p, ['shots'], 0),
                'Shots On Target': _first_present(p, ['shotsOnGoal','shotsOnTarget'], 0),
                'Yellow Cards': _first_present(p, ['yellowCards'], 0),
                'Red Cards': _first_present(p, ['redCards'], 0),
                'Saves': 0,
                'Goals Against': 0,
            }
            if full_name in gk_by_name:
                row['Saves'] = gk_by_name[full_name]['Saves']
                row['Goals Against'] = gk_by_name[full_name]['Goals Against']
            out.append(row)

    return out

def collect_players_from_games(game_ids):
    players_all = []
    for gid in game_ids:
        url = f"https://data.ncaa.com/casablanca/game/{gid}/boxscore.json"
        data = None

        try:
            r = SESSION.get(url, headers={"Referer": f"https://www.ncaa.com/game/{gid}"}, timeout=20)
            if r.status_code == 200:
                data = r.json()
        except requests.RequestException:
            data = None

        if data is None:
            try:
                result = os.popen(
                    f'curl -s -H "User-Agent: {UA}" -H "Referer: https://www.ncaa.com/game/{gid}" {url}'
                ).read()
                data = json.loads(result)
            except Exception:
                data = None

        if not data or 'meta' not in data or 'teams' not in data:
            continue

        players_all.extend(clean_data_from_boxscore_payload(data, gid))
        time.sleep(0.1)
    return players_all


# ====================
# PBP → Fouls Won (PER GAME)
# ====================
def extract_player(event):
    pattern = r'\b[A-Z][a-z]+,?\s*[A-Z][a-z]+'
    matches = re.findall(pattern, event or "")
    return matches[0] if matches else None

def categorize_event(event):
    if not event:
        return 'Other'
    if 'Goal by' in event:
        return 'Goal'
    elif 'Shot by' in event:
        return 'Shot'
    elif 'Foul on' in event or 'Foul' in event:
        return 'Foul'
    elif 'Corner kick' in event:
        return 'Corner Kick'
    elif 'Offside' in event:
        return 'Offside'
    else:
        return 'Other'

def collect_fouls_won(game_ids):
    foul_data = []
    for game_id in game_ids:
        data = None
        try:
            response = SESSION.get(f'https://data.ncaa.com/casablanca/game/{game_id}/pbp.json', timeout=20)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching PBP via requests for game {game_id}: {e}")
            try:
                result = os.popen(
                    f'curl -s https://data.ncaa.com/casablanca/game/{game_id}/pbp.json'
                ).read()
                data = json.loads(result)
            except Exception as e2:
                print(f"Error fetching PBP via curl for game {game_id}: {e2}")
                continue

        if not data or 'meta' not in data or 'periods' not in data:
            print(f"Invalid PBP for game {game_id}")
            continue

        home = data['meta']['teams'][0]['shortName']
        away = data['meta']['teams'][1]['shortName']

        events = []
        score = '0-0'
        for period in data.get('periods', []) or []:
            for play in period.get('playStats', []) or []:
                score = play.get('score') or score
                tm = play.get('time', '')
                if play.get('visitorText'):
                    team_side = 1
                    event = play.get('visitorText', '')
                else:
                    team_side = 0
                    event = play.get('homeText', '')
                events.append({'Score': score, 'Time': tm, 'Event': event, 'Team': team_side})

        if not events:
            continue

        df = pd.DataFrame(events)
        df['Name'] = df['Event'].apply(extract_player).map(clean_name)
        df['Event_Type'] = df['Event'].apply(categorize_event)
        df['Team'] = df['Team'].apply(lambda x: home if x == 0 else away)
        df['IsFoul'] = df['Event'].str.contains('Foul', case=False, na=False)

        foul_df = df[df['IsFoul'] & df['Name'].notna()].copy()
        if foul_df.empty:
            continue

        foul_summary = (foul_df.groupby(['Name', 'Team'])
                        .size().reset_index(name='Fouls'))
        foul_summary['Game ID'] = game_id  # per-game fouls
        foul_data.append(foul_summary)

    if foul_data:
        return pd.concat(foul_data, ignore_index=True)
    else:
        return pd.DataFrame(columns=['Game ID','Name','Team','Fouls'])


# ====================
# Data Collection for a Date Range
# ====================
def collect_player_data_for_period(start_date, end_date):
    """Collect player data for a specific date range"""
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    time_range = date_range.strftime('%m/%d').tolist()

    SEEN_GIDS = {'men': set(), 'women': set()}
    TEAM_DIV_VOTES = {'men': defaultdict(Counter), 'women': defaultdict(Counter)}
    PROCESSED_GIDS = set()

    all_player_rows = []

    for gender in GENDERS:
        for division in DIVISIONS:
            for day in time_range:
                day_games = get_day_games(gender, day, division)
                if not day_games:
                    continue

                # team→division votes
                for g in day_games:
                    for t in g['teams']:
                        TEAM_DIV_VOTES[gender][str(t['id'])][division] += 1

                # fetch each gid once per gender
                new_gids = [g['gid'] for g in day_games if g['gid'] not in SEEN_GIDS[gender]]
                if not new_gids:
                    continue

                rows = collect_players_from_games(new_gids)
                if rows:
                    df = pd.DataFrame(rows)
                    df['Gender'] = gender
                    df['Division'] = division  # temporary; will be overwritten by true division
                    all_player_rows.append(df)

                SEEN_GIDS[gender].update(new_gids)
                PROCESSED_GIDS.update(new_gids)

    # Combine & de-dup per (game/team/player/gender)
    if all_player_rows:
        players_raw = pd.concat(all_player_rows, ignore_index=True)
        players_raw = players_raw.drop_duplicates(subset=['Game ID','Team ID','Name','Gender'])
    else:
        players_raw = pd.DataFrame(columns=[
            'Game ID','Team ID','Name','Team','Position', 'Matches Played','Minutes Played','Goals','Assists','Shots','Shots On Target',
            'Yellow Cards','Red Cards','Saves','Goals Against','Gender','Division'
        ])

    # True team division from votes (ties: d1 > d2 > d3)
    ORDER = {'d1': 0, 'd2': 1, 'd3': 2}
    TEAM_TRUE_DIV = {}
    for gdr, bucket in TEAM_DIV_VOTES.items():
        for tid, counter in bucket.items():
            if counter:
                winners = sorted(counter.items(), key=lambda kv: (-kv[1], ORDER.get(kv[0], 9)))
                TEAM_TRUE_DIV[(gdr, str(tid))] = winners[0][0]

    if not players_raw.empty:
        players_raw['Division'] = players_raw.apply(
            lambda r: TEAM_TRUE_DIV.get((r['Gender'], str(r['Team ID'])), r['Division']),
            axis=1
        )
        players_raw['Name'] = players_raw['Name'].map(clean_name)

    # Merge Fouls Won (PER GAME) with team-scoped fuzzy matching
    fouls_df = collect_fouls_won(list(PROCESSED_GIDS))
    if not players_raw.empty and not fouls_df.empty:
        players_raw['NameKey'] = players_raw['Name'].fillna('').map(preprocess_name)
        fouls_df['Name'] = fouls_df['Name'].map(clean_name)
        fouls_df['NameKey'] = fouls_df['Name'].fillna('').map(preprocess_name)

        players_raw['NameKeyStd'] = players_raw['NameKey']
        fouls_df['NameKeyStd'] = fouls_df['NameKey']

        teams_union = sorted(set(players_raw['Team'].dropna().unique()).union(set(fouls_df['Team'].dropna().unique())))
        for team in teams_union:
            p_mask = players_raw['Team'] == team
            f_mask = fouls_df['Team'] == team
            p_names = players_raw.loc[p_mask, 'Name'].tolist()
            f_names = fouls_df.loc[f_mask, 'Name'].tolist()
            if not p_names and not f_names:
                continue
            name_map = create_name_mapping(p_names, f_names, similarity_threshold=90)
            players_raw.loc[p_mask, 'NameKeyStd'] = apply_name_mapping(p_names, name_map)
            fouls_df.loc[f_mask, 'NameKeyStd'] = apply_name_mapping(f_names, name_map)

        players_raw = players_raw.merge(
            fouls_df[['Game ID','Team','NameKeyStd','Fouls']],
            on=['Game ID','Team','NameKeyStd'],
            how='left'
        )
        players_raw['Fouls Won'] = pd.to_numeric(players_raw['Fouls'], errors='coerce').fillna(0).astype(int)
        players_raw.drop(columns=['Fouls','NameKey','NameKeyStd'], errors='ignore', inplace=True)
    else:
        players_raw['Fouls Won'] = 0

    # ====================
    # Aggregate WITHOUT Dominant Position in the key
    # Then compute Dominant Position ONCE from all positions seen
    # ====================
    num_cols = [
        'Matches Played', 'Minutes Played','Goals','Assists','Shots','Shots On Target',
        'Yellow Cards','Red Cards','Saves','Goals Against','Fouls Won'
    ]
    for c in num_cols:
        if c in players_raw.columns:
            players_raw[c] = pd.to_numeric(players_raw[c], errors='coerce').fillna(0)

    key_cols = ['Name','Team','Gender','Division']

    # Aggregate stats
    player_stats = (players_raw
                    .groupby(key_cols, as_index=False)[num_cols]
                    .sum())

    # Collect raw positions seen per player over all games (optional but useful)
    positions_seen = (players_raw
                      .groupby(key_cols)['Position']
                      .apply(lambda s: ', '.join(sorted({str(x).strip() for x in s if pd.notna(x) and str(x).strip()})))
                      .reset_index(name='Positions Seen'))

    player_stats = player_stats.merge(positions_seen, on=key_cols, how='left')

    # Compute Dominant Position ONCE per player across games
    dompos = (players_raw
              .groupby(key_cols)['Position']
              .apply(compute_dominant_position_over_games)
              .reset_index(name='Dominant Position'))

    player_stats = player_stats.merge(dompos, on=key_cols, how='left')

    # ====================
    # Gender splits + GK views (now a single row per player)
    # ====================
    mens_df = player_stats[player_stats['Gender'] == 'men'].reset_index(drop=True)
    womens_df = player_stats[player_stats['Gender'] == 'women'].reset_index(drop=True)

    mens_df['Points'] = mens_df['Goals'] * 2 + mens_df['Assists']
    womens_df['Points'] = womens_df['Goals'] * 2 + womens_df['Assists']

    return mens_df, womens_df


# ====================
# Main execution
# ====================
def main():
    # Create Data directories if they don't exist
    os.makedirs('Data/Players', exist_ok=True)
    
    # Generate weekly periods
    weekly_periods = get_week_periods(START_DATE, END_DATE)
    
    print(f"Processing {len(weekly_periods)} weekly periods:")
    for week in weekly_periods:
        print(f"  Week ending {week['end']} -> file suffix {week['filename']}")
    
    # Process each week
    for week in weekly_periods:
        week_start = week['start']
        week_end = week['end']
        filename_suffix = week['filename']
        
        print(f"\nProcessing week: {week_start} to {week_end}")
        
        # Collect data for this week
        mens_df, womens_df, mens_gk_df, womens_gk_df = collect_player_data_for_period(week_start, week_end)
        
        # Save the data
        mens_filename = f"Data/Players/mens_players_{filename_suffix}.csv"
        womens_filename = f"Data/Players/womens_players_{filename_suffix}.csv"

        
        mens_df.to_csv(mens_filename, index=False)
        womens_df.to_csv(womens_filename, index=False)

        
        print(f"Saved files:")
        print(f"  - {mens_filename} ({len(mens_df)} players)")
        print(f"  - {womens_filename} ({len(womens_df)} players)")


    print("\nWeekly player data collection completed!")


if __name__ == "__main__":
    main()
