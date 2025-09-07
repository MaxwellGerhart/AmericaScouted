import os
import pandas as pd
import glob

# Test data loading
data_dir = 'data/Players'
print(f"Current working directory: {os.getcwd()}")
print(f"Data directory exists: {os.path.exists(data_dir)}")
print(f"Data directory contents: {os.listdir(data_dir) if os.path.exists(data_dir) else 'Directory not found'}")

# Test loading a specific file
test_file = 'data/Players/womens_players_20250903.csv'
print(f"Test file exists: {os.path.exists(test_file)}")

if os.path.exists(test_file):
    df = pd.read_csv(test_file)
    print(f"File shape: {df.shape}")
    print(f"First few rows:")
    print(df.head())
else:
    print("File not found!")
