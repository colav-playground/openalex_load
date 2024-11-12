from pymongo import MongoClient
from joblib import Parallel, delayed
import time
from colombia_cut_dois import colombia_cut_dois
from colombia_cut_minciencias import colombia_cut_minciencias
db_in = "openalex"
db_out = "openalexco"
es_index = "openalex_index"
jobs = 72

backend = "threading"
db = MongoClient()


db[db_out]["works"].create_index({"type": 1, "type_crossref": 1, "id": 1})

# pipeline for works (cuts works for colombia)
pipeline = [
    {"$project": {"_id": 0}},
    {"$match": {"$or": [{"authorships.countries": "CO"}, {
        "authorships.institutions.country_code": "CO"}]}},
    {"$out": {"db": db_out, "coll": "works"}}
]
# referenced works should be added?
print(f"processing works from {db_in}.works to {db_out}.works")
start = time.time()
db[db_in]["works"].aggregate(pipeline)
end = time.time()
print(f"time = {end - start}")

print(f"processing indexes to get works from colombian publishers")
start = time.time()
db[db_in]["works"].create_index("id")
db[db_in]["works"].create_index("locations.source.publisher_id")
db[db_out]["works"].create_index("id")
end = time.time()
print(f"time = {end - start}")


publishers_ids = list(db[db_in]["publishers"].find(
    {"country_codes": "CO"}, {"id": 1}))


def get_pub_works(pid):
    works = list(db[db_in]["works"].find(
        {"locations.source.publisher_id": pid}))
    return works


print(f"processing publishers: getting works for each one")
start = time.time()

pworks = Parallel(n_jobs=jobs, verbose=10, backend=backend)(
    delayed(get_pub_works)(pid["id"]) for pid in publishers_ids)
end = time.time()
print(f"time = {end - start}")
works = []
for pw in pworks:
    works.extend(pw)
del pworks


def process_pwork(work):
    c = db[db_out]["works"].count_documents({"id": work["id"]})
    if c == 0:
        db[db_out]["works"].insert_one(work)


print(f"processing publishers: adding unique works to {db_out}.works ")
start = time.time()
Parallel(n_jobs=jobs, verbose=10, backend=backend)(
    delayed(process_pwork)(work) for work in works)
end = time.time()
print(f"time = {end - start}")

db[db_in]["works"].create_index("doi")

print(f"processing dois cut: adding works to {db_out}.works ")
start = time.time()
# remember to edit global variables in colombia_cut_dois.py
colombia_cut_dois(db_in=db_in, db_out=db_out, backend=backend)
end = time.time()
print(f"time = {end - start}")

print(f"processing minciencias: adding works to {db_out}.works ")
start = time.time()
colombia_cut_minciencias(db_in, db_out, es_index, jobs, backend)
end = time.time()
print(f"time = {end - start}")

print(
    f"post processing: removing accidentally added duplicated works to {db_out}.works ")
pipeline = [
    # los productos se repiten por convocatoria
    {'$group': {'_id': '$id', 'uniquedoc': {'$first': '$$ROOT'}}},
    {'$replaceRoot': {'newRoot': '$uniquedoc'}}
]
out = db[db_out]["works"].aggregate(pipeline)
for i in out:
    db[db_out]["works_tmp"].insert_one(i)
db[db_out]["works"].drop()
db[db_out]["works_tmp"].rename("works")

# añadir a la descarga los works de las revistas colombianas
# con aggregate toma mucho más tiempo por que corre en secuencial.
print(f"processing index from {db_in}.authors ")
db[db_in]["authors"].create_index("id")
pipeline = [
    {"$project": {"_id": 0, "authorships.author.id": 1}},
    {"$unwind": "$authorships"},
    {"$group": {"_id": None, "authors": {"$addToSet": "$authorships.author.id"}}},
    {"$unwind": "$authors"},
    {"$project": {"_id": 0}}
]

authors_ids = db[db_out]["works"].aggregate(pipeline)
authors_ids = list(authors_ids)


def save_author(aid):
    author = db[db_in]["authors"].find_one({"id": aid})
    if author is not None:
        db[db_out]["authors"].insert_one(author)


print(
    f"processing authors from {db_out}.works to {db_out}.authors filtering from {db_in}.authors")
start = time.time()
r = Parallel(n_jobs=jobs, verbose=10, backend=backend, batch_size=100)(
    delayed(save_author)(author["authors"]) for author in authors_ids)
end = time.time()
print(f"time = {end - start}")


# acá filtro lo demás
# concepts
print(f"processing concepts ")
pipeline_copy = [
    {"$match": {}},
    {"$out": {"db": db_out, "coll": "concepts"}}
]
start = time.time()
db[db_in]["concepts"].aggregate(pipeline_copy)
end = time.time()
print(f"time = {end - start}")
# funders
print(f"processing funders ")
pipeline_copy = [
    {"$match": {}},
    {"$out": {"db": db_out, "coll": "funders"}}
]
start = time.time()
db[db_in]["funders"].aggregate(pipeline_copy)
end = time.time()
print(f"time = {end - start}")
# institutions
print(f"processing institutions ")
pipeline_copy = [
    {"$match": {}},
    {"$out": {"db": db_out, "coll": "institutions"}}
]
start = time.time()
db[db_in]["institutions"].aggregate(pipeline_copy)
end = time.time()
print(f"time = {end - start}")
# publishers
print(f"processing publishers ")
pipeline_copy = [
    {"$match": {}},
    {"$out": {"db": db_out, "coll": "publishers"}}
]
start = time.time()
db[db_in]["publishers"].aggregate(pipeline_copy)
end = time.time()
print(f"time = {end - start}")
# sources
print(f"processing sources ")
pipeline_copy = [
    {"$match": {}},
    {"$out": {"db": db_out, "coll": "sources"}}
]
start = time.time()
db[db_in]["sources"].aggregate(pipeline_copy)
end = time.time()
print(f"time = {end - start}")
