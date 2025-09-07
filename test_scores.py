import pandas as pd
import os

# Test loading the CSV file and examining the score values
filepath = os.path.join('data', 'Matches', 'matches_20250903.csv')

if os.path.exists(filepath):
    df = pd.read_csv(filepath)
    print(f"Loaded {len(df)} matches")
    print(f"Columns: {list(df.columns)}")
    
    # Check score column data types
    print(f"\nHome score data type: {df['home_team_score'].dtype}")
    print(f"Away score data type: {df['away_team_score'].dtype}")
    
    # Show some examples
    print(f"\nFirst 10 home scores:")
    for i, score in enumerate(df['home_team_score'].head(10)):
        print(f"  {i}: '{score}' (type: {type(score)}, is_na: {pd.isna(score)})")
    
    print(f"\nFirst 10 away scores:")
    for i, score in enumerate(df['away_team_score'].head(10)):
        print(f"  {i}: '{score}' (type: {type(score)}, is_na: {pd.isna(score)})")
    
    # Check for empty/missing values
    home_empty = df['home_team_score'].isna() | (df['home_team_score'] == '')
    away_empty = df['away_team_score'].isna() | (df['away_team_score'] == '')
    
    print(f"\nEmpty home scores: {home_empty.sum()}")
    print(f"Empty away scores: {away_empty.sum()}")
    
    # Show examples of empty scores
    if home_empty.sum() > 0:
        print(f"\nExamples of matches with empty home scores:")
        empty_examples = df[home_empty][['home_team_short', 'home_team_score', 'away_team_short', 'away_team_score']].head(5)
        print(empty_examples)
        
    # Show examples of 0 scores
    home_zero = df['home_team_score'] == 0
    if home_zero.sum() > 0:
        print(f"\nExamples of matches with 0 home scores:")
        zero_examples = df[home_zero][['home_team_short', 'home_team_score', 'away_team_short', 'away_team_score']].head(5)
        print(zero_examples)
else:
    print(f"File not found: {filepath}")
