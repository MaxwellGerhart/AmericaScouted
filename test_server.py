from flask import Flask
import os
import pandas as pd

app = Flask(__name__)

@app.route('/test')
def test():
    try:
        # Test data loading
        data_dir = 'data/Players'
        cwd = os.getcwd()
        exists = os.path.exists(data_dir)
        
        if exists:
            files = os.listdir(data_dir)
            test_file = 'data/Players/womens_players_20250903.csv'
            if os.path.exists(test_file):
                df = pd.read_csv(test_file)
                return f"""
                <h1>Data Test Results</h1>
                <p>Current directory: {cwd}</p>
                <p>Data directory exists: {exists}</p>
                <p>Files in data directory: {files}</p>
                <p>Test file exists: {os.path.exists(test_file)}</p>
                <p>DataFrame shape: {df.shape}</p>
                <p>First 5 rows:</p>
                <pre>{df.head().to_string()}</pre>
                """
            else:
                return f"Test file not found: {test_file}"
        else:
            return f"Data directory not found: {data_dir}, CWD: {cwd}"
    except Exception as e:
        return f"Error: {str(e)}"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
