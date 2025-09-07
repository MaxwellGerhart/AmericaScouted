# America Scouted - NCAA Soccer Statistics

A Flask web application for viewing NCAA soccer player statistics with weekly data tracking.

## Features

- View men's and women's player statistics
- Weekly data tracking with cumulative stats
- Filter by position, team, division
- Sort by various statistics (points, goals, assists, etc.)
- Responsive design with professional navy blue theme
- Pagination for large datasets

## Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Generate weekly data:
```bash
python weekly_player_data.py
python weekly_match_data.py
```

3. Run the Flask app:
```bash
python app.py
```

The app will be available at `http://localhost:5000`

## Deployment on Render

1. Connect your GitHub repository to Render
2. Create a new Web Service
3. Use the following settings:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python run.py`
   - **Environment**: Python 3

## Data Structure

The app expects CSV files in the following structure:
```
Data/
├── Players/
│   ├── mens_players_20250817.csv
│   ├── womens_players_20250817.csv
│   ├── mens_players_20250824.csv
│   ├── womens_players_20250824.csv
│   └── ...
└── Matches/
    ├── matches_20250817.csv
    ├── matches_20250824.csv
    └── ...
```

## Key Features

- **Week Selection**: Choose any week to see cumulative stats up to that point
- **Gender Toggle**: Switch between men's and women's statistics
- **Advanced Filtering**: Filter by position, team, division
- **Multiple Sort Options**: Sort by points, goals, assists, shots, minutes, saves
- **Responsive Design**: Works on desktop and mobile devices with navy blue theme
- **Position Color Coding**: Visual indicators for player positions

## CSV Columns Expected

### Player Data
- Name, Team, Gender, Division, Dominant Position
- Matches Played, Minutes Played, Goals, Assists
- Shots, Shots On Target, Yellow Cards, Red Cards
- Saves, Goals Against, Fouls Won, Points

## Technology Stack

- **Backend**: Flask (Python)
- **Frontend**: HTML5, CSS3, JavaScript
- **Data Processing**: Pandas
- **Deployment**: Render
- **Styling**: Custom CSS with navy blue theme and dark background
