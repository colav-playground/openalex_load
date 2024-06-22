from mohan.Similarity import Similarity
from pymongo import MongoClient
from kahi_impactu_utils.String import  parse_mathml, parse_html
#toma unos 40mins
es_index = "openalex_index"

#creating the instance
s = Similarity(es_index,es_uri= "http://localhost:9200",
                 es_auth = ('elastic', 'colav'))

#taking openalex as example.
openalex = MongoClient()["openalex_new"]["works"].find({},{"title":1,"primary_location.source":1,"publication_year":1,"biblio":1,"authorships":1,"_id":1})

#example inserting documents to the Elastic Search index.
bulk_size = 100

s.delete_index(es_index)

es_entries = []
counter = 0
count_nones = 0
for i in openalex:
    work = {}
    if i["title"] is None:
        count_nones += 1
        continue
    title = parse_mathml(i["title"])
    title = parse_html(title)
    work["title"] = title
    if "primary_location" in i.keys() and i["primary_location"]:
        if i["primary_location"]["source"]:
            work["source"] = i["primary_location"]["source"]["display_name"]
        work["source"] = ""
    else:
        work["source"] = ""
    work["year"] = i["publication_year"]
    work["volume"] = i["biblio"]["volume"]
    work["issue"] = i["biblio"]["issue"]
    work["first_page"] = i["biblio"]["first_page"]
    work["last_page"] = i["biblio"]["last_page"]
    authors = []
    for author in i['authorships']:
        if "display_name" in author["author"].keys():
            authors.append(author["author"]["display_name"])
    work["authors"] = authors
    
    entry = {"_index": es_index,
                "_id": str(i["_id"]),
                "_source": work}
    es_entries.append(entry)
    if len(es_entries) == bulk_size:
        s.insert_bulk(es_entries)
        es_entries = []

print("total works without title",count_nones)