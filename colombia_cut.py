from pymongo import MongoClient
from joblib import Parallel, delayed
import time

db_in="openalex_new"
db_out="openalexco_new"

db = MongoClient()

##pipeline for works (cuts works for colombia)
pipeline=[
    {"$project":{"_id":0}},
    {"$match":{"$or":[{"authorships.countries":"CO"},{"authorships.institutions.country_code":"CO"}]}},
    {"$out": { "db": db_out, "coll": "works" }} 
]
## referenced works should be added?
print(f"processing works from {db_in}.works to {db_out}.works")
# db[db_out]["works"].drop()
start = time.time()
db[db_in]["works"].aggregate(pipeline)
end = time.time()
print(f"time = {end - start}")

db[db_in]["works"].create_index("id")
db[db_in]["works"].create_index("primary_location.source.publisher_id")



#añadir a la descarga los works de las revistas colombianas
### con aggregate toma mucho más tiempo por que corre en secuencial.
print(f"processing index from {db_in}.authors ")
db[db_in]["authors"].create_index("id")
#authors_ids = db[db_out]["works"].distinct("authorships.author.id")
pipeline=[
    {"$project":{"_id": 0, "authorships.author.id":1}},
    {"$unwind": "$authorships" },
    { "$group": { "_id": None, "authors": { "$addToSet": "$authorships.author.id" } } },
    { "$unwind": "$authors" },
    {"$project":{"_id": 0}}
]

authors_ids = db[db_out]["works"].aggregate(pipeline)
authors_ids = list(authors_ids)
def save_author(aid):
    author = db[db_in]["authors"].find_one({"id":aid})
    if author is not None:
        db[db_out]["authors"].insert_one(author)

print(f"processing authors from {db_out}.works to {db_out}.authors filtering from {db_in}.authors")
# db[db_out]["authors"].drop()
start = time.time()
r = Parallel(n_jobs=20, verbose=10,backend="multiprocessing", batch_size=100)(
    delayed(save_author)(author["authors"]) for author in authors_ids)
end = time.time()
print(f"time = {end - start}")


#acá filtro lo demás
#concepts
print(f"processing concepts ")
pipeline_copy=[
    {"$match":{}},
    {"$out": { "db": db_out, "coll": "concepts" }}
]
start = time.time()
db[db_in]["concepts"].aggregate(pipeline_copy)
end = time.time()
print(f"time = {end - start}")
#funders
print(f"processing funders ")
pipeline_copy=[
    {"$match":{}},
    {"$out": { "db": db_out, "coll": "funders" }}
]
start = time.time()
db[db_in]["funders"].aggregate(pipeline_copy)
end = time.time()
print(f"time = {end - start}")
#institutions
print(f"processing institutions ")
pipeline_copy=[
    {"$match":{}},
    {"$out": { "db": db_out, "coll": "institutions" }}
]
start = time.time()
db[db_in]["institutions"].aggregate(pipeline_copy)
end = time.time()
print(f"time = {end - start}")
#publishers
print(f"processing publishers ")
pipeline_copy=[
    {"$match":{}},
    {"$out": { "db": db_out, "coll": "publishers" }}
]
start = time.time()
db[db_in]["publishers"].aggregate(pipeline_copy)
end = time.time()
print(f"time = {end - start}")
#sources
print(f"processing sources ")
pipeline_copy=[
    {"$match":{}},
    {"$out": { "db": db_out, "coll": "sources" }}
]
start = time.time()
db[db_in]["sources"].aggregate(pipeline_copy)
end = time.time()
print(f"time = {end - start}")
