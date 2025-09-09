from flask import Flask, render_template, request, jsonify, url_for
import pandas as pd
import os
import random
import glob
from datetime import datetime
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MATCH_DIR = os.path.join(BASE_DIR, 'data', 'Matches')
PLAYER_DIR = os.path.join(BASE_DIR, 'data', 'Players')

app = Flask(__name__)
# Enable template auto reload so changes (like new social buttons) appear without manual restart during development
app.config['TEMPLATES_AUTO_RELOAD'] = True
# Reduce static file cache during active development
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.config['ASSET_VERSION'] = '20250907a'
# Verbose template load diagnostics to console
# Disable verbose template loading in production
app.jinja_env.auto_reload = True  # still allow auto reload in dev

@app.route('/_clear_template_cache')
def clear_template_cache():
    try:
        app.jinja_env.cache.clear()
        _logo_index.cache_clear()
        return jsonify({'status': 'cleared'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ===== Logo Support =====
import re
from functools import lru_cache

LOGO_DIR = os.path.join(BASE_DIR, 'static', 'logos')

def _slugify(name: str) -> str:
    """Mimic scraper slugify so we can map team names to saved logo files."""
    if not name:
        return ''
    name = re.sub(r"\s+", " ", str(name)).strip()
    name = name.replace('/', '-')
    name = re.sub(r"[^A-Za-z0-9.\- _()&']", "", name)
    name = name.replace(' ', '_')
    return name

@lru_cache(maxsize=1)
def _logo_index():
    """Return mapping of slug (without extension) -> actual filename (with ext if present)."""
    index = {}
    if os.path.isdir(LOGO_DIR):
        for fname in os.listdir(LOGO_DIR):
            path = os.path.join(LOGO_DIR, fname)
            if os.path.isfile(path):
                stem, ext = os.path.splitext(fname)
                # Store both stem and full filename without ext for quick lookup
                index[stem.lower()] = fname
    return index

def get_logo_path(team_name: str):
    """Return relative static path (logos/filename.ext) if a logo exists for given team name.

    We try many variants to cope with NCAA short names using 'St.' vs stored 'Saint' or 'St._'.
    """
    if not team_name:
        return None
    original = team_name
    slug = _slugify(original)  # e.g. "St._Bonaventure" OR "Mount_St._Mary's"
    if not slug:
        return None
    idx = _logo_index()

    candidates = [slug]

    # Insert missing period after a leading St if file uses St._X
    if re.match(r'^St_', slug, flags=re.IGNORECASE):
        candidates.append(slug.replace('St_', 'St._', 1))

    # Leading St. -> Saint
    if re.match(r'^St\.?_', slug, flags=re.IGNORECASE):
        candidates.append(re.sub(r'^St\.?_', 'Saint_', slug, flags=re.IGNORECASE))

    # Internal tokens St./St between words -> Saint
    if re.search(r'(?i)\bSt\.?_', slug):
        candidates.append(re.sub(r'(?i)\bSt\.?_', 'Saint_', slug))
    if re.search(r'(?i)_St_', slug):
        candidates.append(re.sub(r'(?i)_St_', '_Saint_', slug))

    # Generic punctuation variants
    candidates.extend([
        slug.replace('.', '_'),
        slug.replace('.', ''),
        re.sub(r'[()]', '', slug)
    ])

    # Space-based re-slug with Saint expansion
    if 'St_' in slug:
        space_form = slug.replace('_', ' ')
        saint_space = re.sub(r'(?i)\bSt\.? ', 'Saint ', space_form)
        saint_space_slug = _slugify(saint_space)
        candidates.append(saint_space_slug)

    # Deduplicate preserving order
    seen = set()
    deduped = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            deduped.append(c)

    def try_candidates(index):
        for cand in deduped:
            key = os.path.splitext(cand)[0].lower()
            if key in index:
                return f"logos/{index[key]}"
        return None

    # First pass
    found = try_candidates(idx)
    if found:
        return found
    # If not found, clear cache and rebuild once (handles newly added files after server start)
    _logo_index.cache_clear()
    refreshed = _logo_index()
    found = try_candidates(refreshed)
    if found:
        return found
    # Fuzzy fallback: strip non-alphanumerics and compare
    def norm(s: str):
        return re.sub(r'[^a-z0-9]', '', s.lower())
    candidate_norms = {norm(c): c for c in candidates}
    try:
        for fname in os.listdir(LOGO_DIR):
            stem, _ = os.path.splitext(fname)
            if norm(stem) in candidate_norms:
                return f"logos/{fname}"
    except Exception:
        pass
    return None


class AmericaScoutedApp:
    def __init__(self):
        # Base player data directory (absolute)
        self.data_dir = PLAYER_DIR
        # Cache available weeks on startup
        self.available_weeks = self.get_available_weeks()
    
    def get_available_weeks(self):
        """Get list of available weeks from CSV files"""
        weeks = []
        if os.path.exists(self.data_dir):
            # Check both men's and women's files
            all_files = glob.glob(os.path.join(self.data_dir, '*_players_*.csv'))
            for file in all_files:
                filename = os.path.basename(file)
                week_code = filename.split('_')[-1].replace('.csv', '')
                if len(week_code) == 8:  # YYYYMMDD format
                    try:
                        date_obj = datetime.strptime(week_code, '%Y%m%d')
                        week_data = {
                            'code': week_code,
                            'date': date_obj,
                            'display': date_obj.strftime('%b %d, %Y')
                        }
                        if week_data not in weeks:
                            weeks.append(week_data)
                    except ValueError:
                        continue
        
        return sorted(weeks, key=lambda x: x['date'])
    
    def load_cumulative_data(self, end_week_code, gender='men'):
        """Load and combine data from start through specified week"""
        all_data = []
    # Removed verbose debug prints for cleaner production output
        
        for week in self.available_weeks:
            if week['code'] <= end_week_code:
                # Fix gender naming to match file names
                file_gender = gender + 's' if gender in ['men', 'women'] else gender
                filename = f"{file_gender}_players_{week['code']}.csv"
                filepath = os.path.join(self.data_dir, filename)
                # Silent check for file existence
                
                if os.path.exists(filepath):
                    try:
                        df = pd.read_csv(filepath)
                        all_data.append(df)
                    except Exception as e:
                        # Swallow errors but could be logged with a logging framework
                        pass
                else:
                    pass
        
        if all_data:
            # Combine all data and aggregate by player
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Group by player and sum statistics
            numeric_cols = ['Matches Played', 'Minutes Played', 'Goals', 'Assists', 
                          'Shots', 'Shots On Target', 'Yellow Cards', 'Red Cards', 
                          'Saves', 'Goals Against', 'Fouls Won']
            
            # Ensure numeric columns exist and are numeric
            for col in numeric_cols:
                if col in combined_df.columns:
                    combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0)
            
            # Group by Name, Team, Gender, Division and sum the stats
            group_cols = ['Name', 'Team', 'Gender', 'Division']
            available_group_cols = [col for col in group_cols if col in combined_df.columns]
            available_numeric_cols = [col for col in numeric_cols if col in combined_df.columns]
            
            if available_group_cols and available_numeric_cols:
                aggregated = combined_df.groupby(available_group_cols, as_index=False)[available_numeric_cols].sum()
                
                # Add calculated columns
                if 'Goals' in aggregated.columns and 'Assists' in aggregated.columns:
                    aggregated['Points'] = aggregated['Goals'] * 2 + aggregated['Assists']
                
                # Get dominant position (take the most recent non-null value)
                if 'Dominant Position' in combined_df.columns:
                    position_data = combined_df.groupby(available_group_cols)['Dominant Position'].last().reset_index()
                    aggregated = aggregated.merge(position_data, on=available_group_cols, how='left')
                
                return aggregated
        
        return pd.DataFrame()
    
    def get_position_color(self, position):
        """Return color class based on position"""
        position_colors = {
            'Goalkeeper': 'goalkeeper',
            'Defender': 'defender', 
            'Midfielder': 'midfielder',
            'Forward': 'forward'
        }
        return position_colors.get(position, 'unknown')
    
    def clean_score(self, score):
        """Clean and format score values"""
        if pd.isna(score) or score == '' or str(score).strip() == '':
            return '-'
        try:
            # Convert to float first, then to int if it's a whole number
            float_val = float(score)
            if float_val.is_integer():
                return str(int(float_val))
            else:
                return str(float_val)
        except (ValueError, TypeError):
            return '-'

    def load_match_data(self, gender):
        """Load and combine match data for all available weeks"""
        all_data = []
        
        for week_info in self.available_weeks:
            week_code = week_info['code']
            filename = f'matches_{week_code}.csv'
            filepath = os.path.join(MATCH_DIR, filename)
            
            if os.path.exists(filepath):
                try:
                    df = pd.read_csv(filepath)
                    # Filter by gender if column exists (lowercase column name)
                    if 'gender' in df.columns:
                        df = df[df['gender'].str.lower() == gender.lower()]
                    all_data.append(df)
                except Exception as e:
                    print(f"Error loading {filepath}: {e}")
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            # Drop duplicate games caused by cross-division listings
            if 'boxscore_id' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['boxscore_id'], keep='first')

            # Recompute division from conferences to avoid mislabeling cross-division games
            if 'home_conference' in combined_df.columns and 'away_conference' in combined_df.columns:
                conf_map = {
                    # D1
                    'acc': 'd1', 'america east': 'd1', 'american': 'd1', 'asun': 'd1', 'atlantic 10': 'd1',
                    'big east': 'd1', 'big south': 'd1', 'big ten': 'd1', 'big west': 'd1', 'caa': 'd1',
                    'cusa': 'd1', 'di independent': 'd1', 'horizon': 'd1', 'ivy league': 'd1', 'maac': 'd1',
                    'mvc': 'd1', 'nec': 'd1', 'ovc': 'd1', 'patriot': 'd1', 'socon': 'd1', 'summit league': 'd1',
                    'sun belt': 'd1', 'wac': 'd1', 'wcc': 'd1',
                    # D2
                    'cacc': 'd2', 'ccaa': 'd2', 'conference carolinas': 'd2', 'dii independent': 'd2', 'ecc': 'd2',
                    'g-mac': 'd2', 'gac': 'd2', 'gliac': 'd2', 'glvc': 'd2', 'great northwest': 'd2', 'gulf south': 'd2',
                    'ind': 'd2', 'lone star': 'd2', 'mec': 'd2', 'ne10': 'd2', 'pacwest': 'd2', 'peach belt': 'd2',
                    'psac': 'd2', 'rmac': 'd2', 'sac': 'd2', 'sunshine state': 'd2',
                    # D3
                    'amcc': 'd3', 'american rivers': 'd3', 'asc': 'd3', 'atlantic east': 'd3', 'c2c': 'd3',
                    'cciw': 'd3', 'ccs': 'd3', 'centennial': 'd3', 'cne': 'd3', 'cunyac': 'd3',
                    'diii independent': 'd3', 'empire 8': 'd3', 'great northeast': 'd3', 'hcac': 'd3', 'ind': 'd3',
                    'landmark': 'd3', 'liberty league': 'd3', 'little east': 'd3', 'mac commonwealth': 'd3',
                    'mac freedom': 'd3', 'mascac': 'd3', 'miac': 'd3', 'michigan intercol. ath. assn.': 'd3',
                    'mwc': 'd3', 'nacc': 'd3', 'ncac': 'd3', 'necc': 'd3', 'nescac': 'd3', 'newmac': 'd3',
                    'njac': 'd3', 'non-ncaa org': 'd3', 'north atlantic': 'd3', 'nwc': 'd3', 'oac': 'd3',
                    'odac': 'd3', 'pac': 'd3', 'saa': 'd3', 'scac': 'd3', 'sciac': 'd3', 'skyline': 'd3',
                    'sliac': 'd3', 'sunyac': 'd3', 'uaa': 'd3', 'umac': 'd3', 'united east': 'd3',
                    'usa south': 'd3', 'wiac': 'd3'
                }
                hc = combined_df['home_conference'].astype(str).str.strip().str.lower()
                ac = combined_df['away_conference'].astype(str).str.strip().str.lower()
                home_div = hc.map(conf_map)
                away_div = ac.map(conf_map)
                # Default to blank to avoid mislabeling cross-division as a specific division
                new_div = pd.Series([''] * len(combined_df), index=combined_df.index)
                same_mask = home_div.notna() & away_div.notna() & (home_div == away_div)
                new_div.loc[same_mask] = home_div.loc[same_mask]
                combined_df['division'] = new_div

            return combined_df
        
        return pd.DataFrame()

america_scouted_app = AmericaScoutedApp()

@app.route('/')
def index():
    # Determine latest week code
    weeks = america_scouted_app.available_weeks
    men_top = []
    women_top = []
    men_assist = []
    women_assist = []
    if weeks:
        latest_code = weeks[-1]['code']
        # Helper to extract top scorers
        def top_scorers(gender):
            try:
                df = america_scouted_app.load_cumulative_data(latest_code, gender)
                if df is not None and not df.empty and 'Goals' in df.columns:
                    # Restrict to Division I only if available
                    if 'Division' in df.columns:
                        df = df[df['Division'].astype(str).str.lower() == 'd1']
                        if df.empty:
                            return []
                    # Ensure numeric
                    df['Goals'] = pd.to_numeric(df['Goals'], errors='coerce').fillna(0)
                    # Secondary sort by Points if available, else Assists
                    sort_cols = ['Goals']
                    if 'Points' in df.columns:
                        sort_cols.append('Points')
                    elif 'Assists' in df.columns:
                        sort_cols.append('Assists')
                    df_sorted = df.sort_values(by=sort_cols, ascending=False)
                    top = []
                    for _, r in df_sorted.head(3).iterrows():
                        entry = {
                            'name': r.get('Name'),
                            'team': r.get('Team'),
                            'goals': int(r.get('Goals', 0)),
                            'assists': int(r.get('Assists', 0)) if 'Assists' in df.columns and pd.notna(r.get('Assists')) else 0,
                            'points': int(r.get('Points', 0)) if 'Points' in df.columns and pd.notna(r.get('Points')) else None
                        }
                        logo_rel = get_logo_path(r.get('Team'))
                        if logo_rel:
                            entry['logo_url'] = url_for('static', filename=logo_rel)
                        top.append(entry)
                    return top
            except Exception:
                return []
            return []
        def top_assisters(gender):
            try:
                df = america_scouted_app.load_cumulative_data(latest_code, gender)
                if df is not None and not df.empty and 'Assists' in df.columns:
                    # Restrict to Division I only if available
                    if 'Division' in df.columns:
                        df = df[df['Division'].astype(str).str.lower() == 'd1']
                        if df.empty:
                            return []
                    df['Assists'] = pd.to_numeric(df['Assists'], errors='coerce').fillna(0)
                    if 'Goals' in df.columns:
                        df['Goals'] = pd.to_numeric(df['Goals'], errors='coerce').fillna(0)
                    sort_cols = ['Assists']
                    # Tie-break by Goals then Points
                    if 'Goals' in df.columns:
                        sort_cols.append('Goals')
                    if 'Points' in df.columns:
                        sort_cols.append('Points')
                    df_sorted = df.sort_values(by=sort_cols, ascending=False)
                    top = []
                    for _, r in df_sorted.head(3).iterrows():
                        entry = {
                            'name': r.get('Name'),
                            'team': r.get('Team'),
                            'assists': int(r.get('Assists', 0)),
                            'goals': int(r.get('Goals', 0)) if 'Goals' in df.columns and pd.notna(r.get('Goals')) else 0,
                            'points': int(r.get('Points', 0)) if 'Points' in df.columns and pd.notna(r.get('Points')) else None
                        }
                        logo_rel = get_logo_path(r.get('Team'))
                        if logo_rel:
                            entry['logo_url'] = url_for('static', filename=logo_rel)
                        top.append(entry)
                    return top
            except Exception:
                return []
            return []
        men_top = top_scorers('men')
        women_top = top_scorers('women')
        men_assist = top_assisters('men')
        women_assist = top_assisters('women')
    return render_template('index.html', weeks=weeks,
                           men_top_scorers=men_top, women_top_scorers=women_top,
                           men_top_assisters=men_assist, women_top_assisters=women_assist)


@app.route('/players')
def players():
    week = request.args.get('week', america_scouted_app.available_weeks[-1]['code'] if america_scouted_app.available_weeks else None)
    gender = request.args.get('gender', 'men')
    position = request.args.get('position', 'all')
    team = request.args.get('team', 'all')
    division = request.args.get('division', 'all')
    conference = request.args.get('conference', 'all')
    search = request.args.get('search', '').strip()
    sort_by = request.args.get('sort', 'Points')
    sort_order = request.args.get('order', 'desc')
    page = int(request.args.get('page', 1))
    per_page = 50
    
    if not week:
        return render_template('players.html', players=[], weeks=america_scouted_app.available_weeks, 
                             current_week=None, gender=gender, total_pages=0, current_page=1)
    
    # Load data
    df = america_scouted_app.load_cumulative_data(week, gender)
    
    if df.empty:
        return render_template('players.html', players=[], weeks=america_scouted_app.available_weeks,
                             current_week=week, gender=gender, total_pages=0, current_page=1)

    # Known division corrections (manual overrides)
    division_corrections = {
        'Westmont': 'd2',
    }
    if 'Team' in df.columns and 'Division' in df.columns:
        df['Division'] = df.apply(
            lambda r: division_corrections.get(r['Team'], r['Division']), axis=1
        )

    # Derive conference info if missing by mapping from match data
    if 'Conference' not in df.columns or df['Conference'].isna().all():
        matches_df = america_scouted_app.load_match_data(gender)
        if not matches_df.empty:
            # Build mapping from team short/full names to conference (prefer home/away conference columns)
            team_to_conf = {}
            def add_mapping(team_name, conf):
                if pd.isna(team_name) or pd.isna(conf) or conf == '':
                    return
                key = str(team_name).strip()
                # Preserve first seen mapping; if different mappings occur we keep the first
                if key not in team_to_conf:
                    team_to_conf[key] = conf
            for _, m in matches_df.iterrows():
                add_mapping(m.get('home_team_short'), m.get('home_conference'))
                add_mapping(m.get('home_team_full'), m.get('home_conference'))
                add_mapping(m.get('away_team_short'), m.get('away_conference'))
                add_mapping(m.get('away_team_full'), m.get('away_conference'))
            # Assign conference to players DataFrame
            df['Conference'] = df['Team'].map(team_to_conf)
        else:
            df['Conference'] = None

    # Conference to division authoritative mapping (D1 & D2 provided so far)
    conference_division_map = {
        # D1
        'acc': 'd1', 'america east': 'd1', 'american': 'd1', 'asun': 'd1', 'atlantic 10': 'd1',
        'big east': 'd1', 'big south': 'd1', 'big ten': 'd1', 'big west': 'd1', 'caa': 'd1',
        'cusa': 'd1', 'di independent': 'd1', 'horizon': 'd1', 'ivy league': 'd1', 'maac': 'd1',
        'mvc': 'd1', 'nec': 'd1', 'ovc': 'd1', 'patriot': 'd1', 'socon': 'd1', 'summit league': 'd1',
        'sun belt': 'd1', 'wac': 'd1', 'wcc': 'd1',
        # D2
        'cacc': 'd2', 'ccaa': 'd2', 'conference carolinas': 'd2', 'dii independent': 'd2', 'ecc': 'd2',
        'g-mac': 'd2', 'gac': 'd2', 'gliac': 'd2', 'glvc': 'd2', 'great northwest': 'd2', 'gulf south': 'd2',
        'ind': 'd2', 'lone star': 'd2', 'mec': 'd2', 'ne10': 'd2', 'pacwest': 'd2', 'peach belt': 'd2',
        'psac': 'd2', 'rmac': 'd2', 'sac': 'd2', 'sunshine state': 'd2',
        # D3
        'amcc': 'd3', 'american rivers': 'd3', 'asc': 'd3', 'atlantic east': 'd3', 'c2c': 'd3',
        'cciw': 'd3', 'ccs': 'd3', 'centennial': 'd3', 'cne': 'd3', 'cunyac': 'd3',
        'diii independent': 'd3', 'empire 8': 'd3', 'great northeast': 'd3', 'hcac': 'd3', 'ind': 'd3',
        'landmark': 'd3', 'liberty league': 'd3', 'little east': 'd3', 'mac commonwealth': 'd3',
        'mac freedom': 'd3', 'mascac': 'd3', 'miac': 'd3', 'michigan intercol. ath. assn.': 'd3',
        'mwc': 'd3', 'nacc': 'd3', 'ncac': 'd3', 'necc': 'd3', 'nescac': 'd3', 'newmac': 'd3',
        'njac': 'd3', 'non-ncaa org': 'd3', 'north atlantic': 'd3', 'nwc': 'd3', 'oac': 'd3',
        'odac': 'd3', 'pac': 'd3', 'saa': 'd3', 'scac': 'd3', 'sciac': 'd3', 'skyline': 'd3',
        'sliac': 'd3', 'sunyac': 'd3', 'uaa': 'd3', 'umac': 'd3', 'united east': 'd3',
        'usa south': 'd3', 'wiac': 'd3'
    }
    if 'Conference' in df.columns:
        norm_conf = df['Conference'].astype(str).str.strip().str.lower()
        mapped_div = norm_conf.map(conference_division_map)
        # Only overwrite when mapping exists (avoid nuking original if unknown)
        if 'Division' in df.columns:
            df.loc[mapped_div.notna(), 'Division'] = mapped_div[mapped_div.notna()]
        # NAIA fallback for unknown conferences
        valid_confs = set(conference_division_map.keys())
        unknown_mask = ~norm_conf.isin(valid_confs) | df['Conference'].isna() | (df['Conference'].astype(str).str.strip() == '')
        if 'Division' not in df.columns:
            df['Division'] = None
        df.loc[unknown_mask, 'Division'] = df.loc[unknown_mask, 'Division'].fillna('naia')
        # Normalize to lowercase for consistency
        df['Division'] = df['Division'].str.lower()
    
    # Preserve unfiltered for dropdown sources
    base_df = df.copy()
    teams = sorted(base_df['Team'].dropna().unique()) if 'Team' in base_df.columns else []
    positions = sorted(base_df['Dominant Position'].dropna().unique()) if 'Dominant Position' in base_df.columns else []
    divisions = sorted(base_df['Division'].dropna().unique()) if 'Division' in base_df.columns else []
    if 'Conference' in base_df.columns:
        if division != 'all' and 'Division' in base_df.columns:
            conferences_source = base_df[base_df['Division'] == division]
        else:
            conferences_source = base_df
        conferences = sorted(conferences_source['Conference'].dropna().unique())
    else:
        conferences = []
    
    # Apply filters after getting dropdown options
    if position != 'all' and 'Dominant Position' in df.columns:
        df = df[df['Dominant Position'] == position]
    
    if team != 'all' and 'Team' in df.columns:
        df = df[df['Team'] == team]
    
    if division != 'all' and 'Division' in df.columns:
        df = df[df['Division'] == division]

    if conference != 'all' and 'Conference' in df.columns:
        df = df[df['Conference'] == conference]
    
    # Apply search filter
    if search and 'Name' in df.columns:
        df = df[df['Name'].str.contains(search, case=False, na=False)]
    
    # Sort data
    if sort_by in df.columns:
        ascending = sort_order == 'asc'
        df = df.sort_values(by=sort_by, ascending=ascending)
    
    # Pagination
    total_players = len(df)
    total_pages = (total_players + per_page - 1) // per_page
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    
    players_page = df.iloc[start_idx:end_idx]
    
    # Convert to list of dicts for template
    players = []
    for _, player in players_page.iterrows():
        player_dict = player.to_dict()
        player_dict['position_color'] = america_scouted_app.get_position_color(player.get('Dominant Position', ''))
        # Attach logo path if available
        team_name = player.get('Team')
        logo_rel = get_logo_path(team_name) if team_name else None
        if logo_rel:
            player_dict['logo_url'] = url_for('static', filename=logo_rel)
        # slug for team link
        if team_name:
            player_dict['team_slug'] = _slugify(team_name)
        players.append(player_dict)
    
    current_week_display = next((w['display'] for w in america_scouted_app.available_weeks if w['code'] == week), week)
    
    return render_template('players.html', 
                         players=players,
                         weeks=america_scouted_app.available_weeks,
                         current_week=week,
                         current_week_display=current_week_display,
                         gender=gender,
                         position=position,
                         team=team,
                         division=division,
                         conference=conference,
                         search=search,
                         sort_by=sort_by,
                         sort_order=sort_order,
                         teams=teams,
                         positions=positions,
                         divisions=divisions,
                         conferences=conferences,
                         total_pages=total_pages,
                         current_page=page,
                         total_players=total_players)

@app.route('/matches')
def matches():
    gender = request.args.get('gender', 'men')
    division = request.args.get('division', 'all')
    conference = request.args.get('conference', 'all')
    day_param = request.args.get('day', '').strip()
    
    # Load match data
    df = america_scouted_app.load_match_data(gender)
    
    if df.empty:
        return render_template('matches.html', matches_by_date={}, gender=gender, 
                             divisions=[], conferences=[], division=division, conference=conference,
                             total_matches=0,
                             current_week_display=america_scouted_app.available_weeks[-1]['display'] if america_scouted_app.available_weeks else None)
    
    # Normalize and apply day filter if provided
    selected_day_iso = ''
    if day_param:
        # Accept ISO (YYYY-MM-DD) or MM-DD-YYYY / MM/DD/YYYY
        parsed = None
        for fmt in ('%Y-%m-%d', '%m-%d-%Y', '%m/%d/%Y'):
            try:
                parsed = datetime.strptime(day_param, fmt)
                break
            except Exception:
                continue
        if parsed:
            # CSV uses MM-DD-YYYY
            day_key = parsed.strftime('%m-%d-%Y')
            selected_day_iso = parsed.strftime('%Y-%m-%d')
            if 'date' in df.columns:
                df = df[df['date'] == day_key]
        else:
            # If unparsable, leave unfiltered
            selected_day_iso = ''
    
    # Get unique values for filter dropdowns (post day filter)
    divisions = sorted(df['division'].unique()) if 'division' in df.columns else []
    conferences = []
    if 'home_conference' in df.columns and 'away_conference' in df.columns:
        home_conferences = df['home_conference'].dropna().unique()
        away_conferences = df['away_conference'].dropna().unique()
        conferences = sorted(set(list(home_conferences) + list(away_conferences)))
    
    # Apply filters
    if division != 'all' and 'division' in df.columns:
        df = df[df['division'] == division]
    
    if conference != 'all':
        if 'home_conference' in df.columns and 'away_conference' in df.columns:
            df = df[(df['home_conference'] == conference) | (df['away_conference'] == conference)]
    
    # Sort by date (most recent first)
    if 'date' in df.columns:
        df = df.sort_values(by='date', ascending=False)
    
    # Group matches by date
    matches_by_date = {}
    for _, match in df.iterrows():
        date = match.get('date', 'Unknown Date')
        if date not in matches_by_date:
            matches_by_date[date] = []
        
        # Create match object with correct column names and handle NaN values
        match_obj = {
            'home_team_short': match.get('home_team_short', 'N/A') if pd.notna(match.get('home_team_short')) else 'N/A',
            'away_team_short': match.get('away_team_short', 'N/A') if pd.notna(match.get('away_team_short')) else 'N/A',
            'home_team_score': america_scouted_app.clean_score(match.get('home_team_score')),
            'away_team_score': america_scouted_app.clean_score(match.get('away_team_score')),
            'home_conference': match.get('home_conference', '') if pd.notna(match.get('home_conference')) else '',
            'away_conference': match.get('away_conference', '') if pd.notna(match.get('away_conference')) else '',
            'division': match.get('division', '') if pd.notna(match.get('division')) else '',
            'status': match.get('status', '') if pd.notna(match.get('status')) else '',
            'start_time': match.get('start_time', '') if pd.notna(match.get('start_time')) else ''
        }
        # Add logos for short name first, fallback to full if available
        h_logo = get_logo_path(match.get('home_team_short') or match.get('home_team_full'))
        a_logo = get_logo_path(match.get('away_team_short') or match.get('away_team_full'))
        if h_logo:
            match_obj['home_logo_url'] = url_for('static', filename=h_logo)
        if a_logo:
            match_obj['away_logo_url'] = url_for('static', filename=a_logo)
        # Add slugs for linking to team page
        match_obj['home_team_slug'] = _slugify(match.get('home_team_short') or match.get('home_team_full') or '')
        match_obj['away_team_slug'] = _slugify(match.get('away_team_short') or match.get('away_team_full') or '')
        
        # Debug output for scores
        
        matches_by_date[date].append(match_obj)
    
    total_matches = len(df)
    current_week_display = america_scouted_app.available_weeks[-1]['display'] if america_scouted_app.available_weeks else None
    
    return render_template('matches.html',
                         matches_by_date=matches_by_date,
                         gender=gender,
                         division=division,
                         conference=conference,
                         divisions=divisions,
                         conferences=conferences,
                         total_matches=total_matches,
                         current_week_display=current_week_display,
                         selected_day_iso=selected_day_iso)

@app.route('/team/<team_slug>')
def team_page(team_slug):
    """Team detail page: record, goals, shots, TSR, etc."""
    gender = request.args.get('gender', 'men')
    # Load all matches for this gender
    matches_df = america_scouted_app.load_match_data(gender)
    if matches_df.empty:
        return render_template('team.html', team_name=team_slug.replace('_', ' '), gender=gender, stats=None, matches=[], error='No match data available.')

    # Helper to coerce numeric values
    def num(val):
        try:
            if val in [None, '', 'nan']:
                return 0
            v = float(val)
            # treat ints
            if v.is_integer():
                return int(v)
            return v
        except Exception:
            return 0

    # Build slugs for matching
    slugged_rows = []
    for _, row in matches_df.iterrows():
        home_short = row.get('home_team_short')
        home_full = row.get('home_team_full')
        away_short = row.get('away_team_short')
        away_full = row.get('away_team_full')
        slug_home = _slugify(home_short or home_full or '')
        slug_away = _slugify(away_short or away_full or '')
        if slug_home == team_slug or slug_away == team_slug:
            slugged_rows.append(row)

    if not slugged_rows:
        return render_template('team.html', team_name=team_slug.replace('_', ' '), gender=gender, stats=None, matches=[], error='Team not found in matches.')

    # Aggregate stats
    wins = draws = losses = 0
    gf = ga = 0
    shots_for = shots_against = 0
    sot_for = sot_against = 0
    division = conference = None
    team_display_name = None

    match_items = []
    for row in slugged_rows:
        home_short = row.get('home_team_short')
        home_full = row.get('home_team_full')
        away_short = row.get('away_team_short')
        away_full = row.get('away_team_full')
        slug_home = _slugify(home_short or home_full or '')
        slug_away = _slugify(away_short or away_full or '')
        is_home = slug_home == team_slug

        # Scores
        h_sc = num(row.get('home_team_score'))
        a_sc = num(row.get('away_team_score'))
        # Shots
        h_sh = num(row.get('home_shots'))
        a_sh = num(row.get('away_shots'))
        h_sot = num(row.get('home_sot'))
        a_sot = num(row.get('away_sot'))

        if is_home:
            goals_for = h_sc
            goals_against = a_sc
            sh_for = h_sh
            sh_against = a_sh
            sotf = h_sot
            sota = a_sot
            opponent_name = away_short or away_full
            opponent_slug = slug_away
        else:
            goals_for = a_sc
            goals_against = h_sc
            sh_for = a_sh
            sh_against = h_sh
            sotf = a_sot
            sota = h_sot
            opponent_name = home_short or home_full
            opponent_slug = slug_home

        # Only count result if both scores present (avoid future or incomplete games)
        if isinstance(goals_for, (int, float)) and isinstance(goals_against, (int, float)):
            if goals_for > goals_against:
                wins += 1
            elif goals_for == goals_against:
                draws += 1
            else:
                losses += 1

        gf += goals_for
        ga += goals_against
        shots_for += sh_for
        shots_against += sh_against
        sot_for += sotf
        sot_against += sota

        if not division:
            division = row.get('division')
        if not conference:
            # prefer home conference when home else away
            conference = row.get('home_conference') if is_home else row.get('away_conference')
        if not team_display_name:
            team_display_name = (home_short or home_full) if is_home else (away_short or away_full)

        match_items.append({
            'date': row.get('date'),
            'is_home': is_home,
            'opponent': opponent_name,
            'opponent_slug': opponent_slug,
            'score_for': goals_for,
            'score_against': goals_against,
            'shots_for': sh_for,
            'shots_against': sh_against,
            'sot_for': sotf,
            'sot_against': sota,
            'result': 'W' if goals_for > goals_against else ('D' if goals_for == goals_against else 'L')
        })

    # Sort matches by date (assuming MM-DD-YYYY as in data) convert to datetime safely
    try:
        match_items.sort(key=lambda m: datetime.strptime(m['date'], '%m-%d-%Y'))
    except Exception:
        pass

    tsr = round(shots_for / (shots_for + shots_against), 3) if (shots_for + shots_against) > 0 else 0
    sotr = round(sot_for / (sot_for + sot_against), 3) if (sot_for + sot_against) > 0 else 0

    stats = {
        'record': f"{wins}-{losses}-{draws}",
        'wins': wins,
        'losses': losses,
        'draws': draws,
        'gf': gf,
        'ga': ga,
        'gd': gf - ga,
        'shots_for': shots_for,
        'shots_against': shots_against,
        'tsr': tsr,
        'sot_for': sot_for,
        'sot_against': sot_against,
        'sotr': sotr,
        'division': division,
        'conference': conference
    }

    # Logo
    logo_rel = get_logo_path(team_display_name)
    logo_url = url_for('static', filename=logo_rel) if logo_rel else None

    return render_template('team.html',
                           team_name=team_display_name or team_slug.replace('_', ' '),
                           gender=gender,
                           stats=stats,
                           matches=match_items,
                           logo_url=logo_url,
                           team_slug=team_slug)

@app.route('/api/player/<player_name>')
def player_detail(player_name):
    week = request.args.get('week', america_scouted_app.available_weeks[-1]['code'] if america_scouted_app.available_weeks else None)
    gender = request.args.get('gender', 'men')
    
    if not week:
        return jsonify({'error': 'No week specified'})
    
    df = america_scouted_app.load_cumulative_data(week, gender)
    
    if df.empty:
        return jsonify({'error': 'No data found'})
    
    player_data = df[df['Name'] == player_name]
    
    if player_data.empty:
        return jsonify({'error': 'Player not found'})
    
    return jsonify(player_data.iloc[0].to_dict())

if __name__ == '__main__':
    # Turn debug back on for development so template / asset changes show immediately
    app.run(debug=True, host='0.0.0.0', port=5000)
