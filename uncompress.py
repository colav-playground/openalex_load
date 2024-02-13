import glob
from joblib import Parallel, delayed
import os

def uncompress(file_gz):
    command = f"gzip -f -d {file_gz}"
    os.system(command)

files = glob.glob("*/*/*.gz",recursive=True)

Parallel(n_jobs=20, backend="multiprocessing")(delayed(uncompress)(file) for file in files)
