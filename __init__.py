import subprocess
import os
import sys

def run_fastapi():
    subprocess.Popen([sys.executable, 'routes/routes.py']) 

def run_streamlit():
    subprocess.Popen(['streamlit', 'run', 'streamlitPages/page1.py'])

def run_mlflow():
    subprocess.Popen(['mlflow', 'ui']) 

if __name__ == '__main__':
    run_fastapi()
    run_streamlit()
    run_mlflow()

    print("Todos os serviços estão rodando...")
    print("Fast API em: http://127.0.0.1:8000/")
    print("Streamlit em: http://127.0.0.1:8501/")
    print("Mlflow em: http://127.0.0.1:5000/")
