import subprocess
import sys
import os

def main():
    port = int(os.environ.get('PORT', 8501))
    subprocess.run([
        sys.executable, '-m', 'streamlit', 'run', 'app.py',
        '--server.port', str(port),
        '--server.address', '0.0.0.0'
    ])

if __name__ == '__main__':
    main()