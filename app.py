from flask import Flask, render_template, request, jsonify, url_for
import pandas as pd
import os
import glob
from datetime import datetime
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MATCH_DIR = os.path.join(BASE_DIR, 'data', 'Matches')
PLAYER_DIR = os.path.join(BASE_DIR, 'data', 'Players')

app = Flask(__name__)

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
    """Return relative static path (logos/filename.ext) if a logo exists for given team name."""
    slug = _slugify(team_name)
    if not slug:
        return None
    idx = _logo_index()
    # Try direct slug, then variations removing periods or parentheses
    candidates = [slug, slug.replace('.', '_'), slug.replace('.', ''), re.sub(r'[()]', '', slug)]
    for cand in candidates:
        key = os.path.splitext(cand)[0].lower()
        if key in idx:
            return f"logos/{idx[key]}"  # relative to /static
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
        print(f"Loading data for {gender}, data_dir: {self.data_dir}")
        print(f"Available weeks: {self.available_weeks}")
        
        for week in self.available_weeks:
            if week['code'] <= end_week_code:
                # Fix gender naming to match file names
                file_gender = gender + 's' if gender in ['men', 'women'] else gender
                filename = f"{file_gender}_players_{week['code']}.csv"
                filepath = os.path.join(self.data_dir, filename)
                print(f"Checking file: {filepath}")
                
                if os.path.exists(filepath):
                    try:
                        df = pd.read_csv(filepath)
                        print(f"Loaded {filepath} with shape: {df.shape}")
                        all_data.append(df)
                    except Exception as e:
                        print(f"Error loading {filepath}: {e}")
                else:
                    print(f"File does not exist: {filepath}")
        
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
            return combined_df
        
        return pd.DataFrame()

america_scouted_app = AmericaScoutedApp()

@app.route('/')
def index():
    return render_template('index.html', weeks=america_scouted_app.available_weeks)

@app.route('/players')
def players():
    print("=== PLAYERS ROUTE CALLED ===")
    week = request.args.get('week', america_scouted_app.available_weeks[-1]['code'] if america_scouted_app.available_weeks else None)
    gender = request.args.get('gender', 'men')
    print(f"Parameters: week={week}, gender={gender}")
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
    print("=== MATCHES ROUTE CALLED ===")
    gender = request.args.get('gender', 'men')
    division = request.args.get('division', 'all')
    conference = request.args.get('conference', 'all')
    
    print(f"Parameters: gender={gender}, division={division}, conference={conference}")
    
    # Load match data
    df = america_scouted_app.load_match_data(gender)
    
    if df.empty:
        return render_template('matches.html', matches_by_date={}, gender=gender, 
                             divisions=[], conferences=[], division=division, conference=conference,
                             total_matches=0,
                             current_week_display=america_scouted_app.available_weeks[-1]['display'] if america_scouted_app.available_weeks else None)
    
    # Get unique values for filter dropdowns
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
        
        # Debug output for scores
        print(f"DEBUG: Original scores - home: '{match.get('home_team_score')}' (type: {type(match.get('home_team_score'))}), away: '{match.get('away_team_score')}' (type: {type(match.get('away_team_score'))})")
        print(f"DEBUG: Processed scores - home: '{match_obj['home_team_score']}', away: '{match_obj['away_team_score']}'")
        
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
                         current_week_display=current_week_display)

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


def _startup_diagnostics():
    try:
        match_files = glob.glob(os.path.join(MATCH_DIR, 'matches_*.csv'))
        print(f"[STARTUP] Base dir: {BASE_DIR}")
        print(f"[STARTUP] Matches dir exists: {os.path.exists(MATCH_DIR)} files={len(match_files)}")
        if match_files:
            print('[STARTUP] Sample file:', os.path.basename(sorted(match_files)[-1]))
        print(f"[STARTUP] Players dir exists: {os.path.exists(PLAYER_DIR)}")
    except Exception as e:
        print('[STARTUP] Diagnostic error', e)

if __name__ == '__main__':
    if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        _startup_diagnostics()
    app.run(debug=True, host='0.0.0.0', port=5000)
