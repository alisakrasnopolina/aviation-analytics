"""Последовательность: существующий конвейер -> прогноз из сохранённых моделей."""
import argparse
from pathlib import Path
import subprocess
import sys


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-dir',required=True)
    p.add_argument('--model-dir',default=None)
    args=p.parse_args()
    root=Path(__file__).resolve().parents[1]
    subprocess.run([sys.executable,'-m','src.main'],cwd=root,check=True)
    command=[sys.executable,'-m','src.forecasting','predict','--run-dir',args.run_dir]
    if args.model_dir:command.extend(['--model-dir',args.model_dir])
    subprocess.run(command,cwd=root,check=True)


if __name__=='__main__':main()
