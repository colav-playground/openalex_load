import glob
from joblib import Parallel, delayed
import os

db_name = "openalex_new"
entities = ('authors',  'concepts',  'funders',  'institutions',  'merged_ids',  'publishers',  'sources',  'works')

def load(file, entity):
    command = f"mongoimport -d {db_name} -c {entity} --type json --file  $i {file}"
    os.system(command)
    os.unlink(file)


for entity in entities:
    files = glob.glob(f"data/{entity}/*/*",recursive=True)
    Parallel(n_jobs=20, backend="multiprocessing")(delayed(load)(file,entity) for file in files)
