import glob
from joblib import Parallel, delayed
import os

db_name = "openalex"
entities = ("authors", "concepts", "domains", "fields", "funders", "institutions", "merged_ids", "publishers", "sources", "subfields", "topics", "works")

def load(file, entity):
    command = f"mongoimport -d {db_name} -c {entity} --type json --file  $i {file}"
    os.system(command)
    os.unlink(file)


for entity in entities:
    files = glob.glob(f"data/{entity}/*/*",recursive=True)
    Parallel(n_jobs=72, backend="multiprocessing")(delayed(load)(file,entity) for file in files)
